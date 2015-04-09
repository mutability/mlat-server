# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import zlib
import logging
import json
import struct

from . import util
from . import latlon


@asyncio.coroutine
def start_client(r, w):
    client = MlatClient(r, w)
    yield from client.run()


class MlatClient:
    def __init__(self, reader, writer):
        self.r = reader
        self.w = writer
        self.transport = writer.transport
        self.compression_methods = (
            ('zlib', self.handle_zlib_messages),
            ('none', self.handle_line_messages),
        )

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
                self.use_heartbeats = bool(hs.get('heartbeat', False))
                self.use_selective_traffic = bool(hs.get('selective_traffic', False))
                self.use_return_results = bool(hs.get('return_results', False))

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
                   selective_traffic=self.use_selective_traffic,
                   heartbeat=self.use_heartbeats,
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
                    yield from self.process_message(line)

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

    @asyncio.coroutine
    def process_message(self, line):
        msg = json.loads(line)
        logging.debug('receive %s', msg)
