# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import zlib
import logging
import json
import struct

from . import util
from . import latlon


@asyncio.coroutine
def start_client(r, w, **kwargs):
    client = MlatClient(r, w, **kwargs)
    yield from client.run()


class MlatClient:
    def __init__(self, reader, writer, *, coordinator):
        self.r = reader
        self.w = writer
        self.coordinator = coordinator
        self.transport = writer.transport
        self.compression_methods = (
            ('zlib', self.handle_zlib_messages),
            ('none', self.handle_line_messages),
        )
        self.client_id = None

    @asyncio.coroutine
    def run(self):
        host, port = self.transport.get_extra_info('peername')
        logging.info('Accepted new client connection from %s:%d', host, port)

        try:
            hs = yield from asyncio.wait_for(self.r.readline(), timeout=30.0)
            if not self.process_handshake(hs):
                return

            yield from self.handle_messages()

        except Exception:
            logging.exception('Exception handling client')

        finally:
            if self.client_id is not None:
                self.coordinator.client_logout(self.client_id)
            self.transport.close()

    def process_handshake(self, line):
        deny = None

        try:
            hs = json.loads(line.decode('ascii'))
        except ValueError as e:
            deny = 'Badly formatted handshake: ' + str(e)
        else:
            try:
                if hs['version'] != 2:
                    raise ValueError('Unsupported version in handshake')

                peer_compression_methods = set(hs['compress'])
                self.compress = None
                for c, m in self.compression_methods:
                    if c in peer_compression_methods:
                        self.compress = c
                        self.handle_messages = m
                        break
                if self.compress is None:
                    raise ValueError('No mutually usable compression type')

                self.lat = float(hs['lat'])
                if self.lat < -90 or self.lat > 90:
                    raise ValueError('invalid latitude, should be -90 .. 90')

                self.lon = float(hs['lon'])
                if self.lon < -180 or self.lon > 360:
                    raise ValueError('invalid longitude, should be -180 .. 360')
                if self.lon > 180:
                    self.lon = self.lon - 180

                self.alt = float(hs['alt'])
                if self.alt < -1000 or self.alt > 10000:
                    raise ValueError('invalid altitude, should be -1000 .. 10000')

                self.ecef = latlon.llh2ecef(self.lat, self.lon, self.alt)

                self.user = str(hs['user'])

                if not hs.get('heartbeat', False):
                    raise ValueError('must use heartbeats')

                if not hs.get('selective_traffic', False):
                    raise ValueError('must use selective traffic')

                self.use_return_results = bool(hs.get('return_results', False))

                self.client_id = self.coordinator.client_login(self, self.user)

            except KeyError as e:
                deny = 'Missing field in handshake: ' + str(e)

            except ValueError as e:
                deny = 'Bad values in handshake: ' + str(e)

        if deny:
            logging.info('Handshake failed: %s', deny)
            self.write({'deny': [deny], 'reconnect_in': util.fuzzy(900)})
            return False

        # todo: MOTD
        self.write(compress=self.compress,
                   reconnect_in=util.fuzzy(60),
                   selective_traffic=True,
                   heartbeat=True,
                   return_results=self.use_return_results)

        return True

    def write(self, **kwargs):
        self.w.write((json.dumps(kwargs) + '\n').encode('ascii'))

    @asyncio.coroutine
    def handle_line_messages(self):
        while not self.r.at_eof():
            line = yield from self.r.readline()
            if not line:
                return
            yield from self.process_message(line)

    @asyncio.coroutine
    def handle_zlib_messages(self):
        decompressor = zlib.decompressobj()

        while not self.r.at_eof():
            header = (yield from self.r.readexactly(2))
            hlen, = struct.unpack('!H', header)

            packet = (yield from self.r.readexactly(hlen))
            packet += b'\x00\x00\xff\xff'

            linebuf = ''
            decompression_done = False
            while not decompression_done:
                # limit decompression to 64k at a time
                if packet:
                    decompressed = decompressor.decompress(packet, 65536)
                    if not decompressed:
                        raise ValueError('Decompressor made no progress')
                    packet = decompressor.unconsumed_tail
                else:
                    decompressed = decompressor.flush()
                    decompression_done = True

                linebuf += decompressed.decode('ascii')
                lines = linebuf.split('\n')
                for line in lines[:-1]:
                    self.process_message(line)

                linebuf = lines[-1]
                if len(linebuf) > 1024:
                    raise ValueError('Client sent a very long line')

                if packet:
                    # try to mitigate DoS attacks that send highly compressible data
                    yield from asyncio.sleep(0.1)

            if decompressor.unused_data:
                raise ValueError('Client sent a packet that had trailing uncompressed data')
            if linebuf:
                raise ValueError('Client sent a packet that was not newline terminated')

    def process_message(self, line):
        msg = json.loads(line)

        if 'sync' in msg:
            self.process_sync_message(msg['sync'])
        elif 'mlat' in msg:
            self.process_mlat_message(msg['mlat'])
        elif 'seen' in msg:
            self.process_seen_message(msg['seen'])
        elif 'lost' in msg:
            self.process_lost_message(msg['lost'])
        elif 'input_connected' in msg:
            self.process_input_connected_message(msg['input_connected'])
        elif 'input_disconnect' in msg:
            self.process_input_disconnect_message(msg['input_disconnect'])
        elif 'heartbeat' in msg:
            self.process_heartbeat_message(msg['heartbeat'])
        else:
            logging.info('Received an unexpected message: %s', msg)

    def process_sync_message(self, sync):
        even_time = float(sync['et'])
        odd_time = float(sync['ot'])
        even_message = bytes.fromhex(sync['em'])
        odd_message = bytes.fromhex(sync['om'])

        self.coordinator.client_sync(self.client_id, even_time, odd_time, even_message, odd_message)

    def process_mlat_message(self, mlat):
        t = float(mlat['t'])
        m = bytes.fromhex(mlat['m'])

        self.coordinator.client_mlat(self.client_id, t, m)

    def process_seen_message(self, seen):
        for icao in seen:
            self.coordinator.client_tracking(self.client_id, icao)

    def process_lost_message(self, lost):
        for icao in lost:
            self.coordinator.client_not_tracking(self.client_id, icao)

    def process_input_connected_message(self, m):
        pass

    def process_input_disconnected_message(self, m):
        self.coordinator.client_reset(self.client_id)

    def process_heartbeat_message(self, m):
        pass
