# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import zlib
import logging
import json
import struct
import time
import random
import socket
import functools

from . import util
from . import geodesy
from . import connection
from .constants import MTOF


@asyncio.coroutine
def start_listeners(tcp_port, udp_port, coordinator, loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    if udp_port is not None:
        dgram_coro = loop.create_datagram_endpoint(protocol_factory=PackedMlatServerProtocol,
                                                   family=socket.AF_INET,
                                                   local_addr=(None, udp_port))
        udp_transport, udp_protocol = (yield from dgram_coro)

    tcp_server = (yield from asyncio.start_server(functools.partial(start_json_client,
                                                                    udp_protocol=udp_protocol,
                                                                    coordinator=coordinator),
                                                  family=socket.AF_INET,
                                                  port=tcp_port))

    return (tcp_server, udp_transport)


def start_json_client(r, w, **kwargs):
    host, port = w.transport.get_extra_info('peername')
    logging.info('Accepted new client connection from %s:%d', host, port)
    client = JsonClient(r, w, **kwargs)
    client.start()


class PackedMlatServerProtocol(asyncio.DatagramProtocol):
    TYPE_SYNC = 1
    TYPE_MLAT_SHORT = 2
    TYPE_MLAT_LONG = 3

    STRUCT_HEADER = struct.Struct(">IQ")
    STRUCT_SYNC = struct.Struct(">ii14s14s")
    STRUCT_MLAT_SHORT = struct.Struct(">i7s")
    STRUCT_MLAT_LONG = struct.Struct(">i14s")

    def __init__(self):
        self.clients = {}
        self._r = random.SystemRandom()
        self.listen_address = None

    def add_client(self, sync_handler, mlat_handler):
        newkey = self._r.getrandbits(32)
        while newkey in self.clients:
            newkey = self._r.getrandbits(32)
        self.clients[newkey] = (sync_handler, mlat_handler)
        return newkey

    def remove_client(self, key):
        self.clients.pop(key, None)

    def connection_made(self, transport):
        self.listen_address = transport.get_extra_info('sockname')

    def datagram_received(self, data, addr):
        try:
            key, base = self.STRUCT_HEADER.unpack_from(data, 0)
            sync_handler, mlat_handler = self.clients[key]  # KeyError on bad client key

            i = self.STRUCT_HEADER.size
            while i < len(data):
                typebyte = data[i]
                i += 1

                if typebyte == self.TYPE_SYNC:
                    et, ot, em, om = self.STRUCT_SYNC.unpack_from(data, i)
                    i += self.STRUCT_SYNC.size
                    sync_handler(base + et, base + ot, em, om)

                elif typebyte == self.TYPE_MLAT_SHORT:
                    t, m = self.STRUCT_MLAT_SHORT.unpack_from(data, i)
                    i += self.STRUCT_MLAT_SHORT.size
                    mlat_handler(base + t, m)

                elif typebyte == self.TYPE_MLAT_LONG:
                    t, m = self.STRUCT_MLAT_LONG.unpack_from(data, i)
                    i += self.STRUCT_MLAT_LONG.size
                    mlat_handler(base + t, m)

                else:
                    # bad data
                    break
        except struct.error:
            pass
        except KeyError:
            pass


class JsonClient(connection.Connection):
    write_heartbeat_interval = 30.0
    read_heartbeat_interval = 45.0

    def __init__(self, reader, writer, *, coordinator, udp_protocol):
        self.r = reader
        self.w = writer
        self.coordinator = coordinator
        self.udp_protocol = udp_protocol
        self._udp_key = None
        self.transport = writer.transport
        self.compression_methods = (
            ('zlib2', self.handle_zlib_messages, self.write_zlib),
            ('zlib', self.handle_zlib_messages, self.write_raw),
            ('none', self.handle_line_messages, self.write_raw)
        )
        self.receiver = None
        self._read_task = None
        self._last_message_time = None

        self._pending_traffic_update = None
        self._requested_traffic = set()
        self._wanted_traffic = set()

        self._compressor = None
        self._pending_flush = None
        self._writebuf = []

    def start(self):
        self._read_task = asyncio.async(self.handle_connection())

    @asyncio.coroutine
    def handle_heartbeats(self):
        """A coroutine that:

        * Periodicallys write heartbeat messages to the client.
        * Monitors when the last message from the client was seen, and closes
        down the connection if the read heartbeat interval is exceeded.

        This coroutine is started as a task from handle_connection() after the
        initial handshake is complete."""

        while True:
            # wait a while..
            yield from asyncio.sleep(self.write_heartbeat_interval)

            # if we have seen no activity recently, declare the
            # connection dead and close it down
            if (time.monotonic() - self._last_message_time) > self.read_heartbeat_interval:
                logging.warn("Client timeout, no recent messages seen, closing connection")
                self._read_task.cancel()  # finally block will do cleanup

            # write a heartbeat message
            self.send(heartbeat=round(time.time(), 3))

    @asyncio.coroutine
    def handle_connection(self):
        """A coroutine that handle reading from the client and processing messages.

        This does the initial handshake, then reads and processes messages
        after the handshake iscomplete.

        It also does any client cleanup needed when the connection is closed.

        This coroutine's task is stashed as self.read_task; cancelling this
        task will cause the client connection to be closed and cleaned up."""

        heartbeat_task = None

        try:
            hs = yield from asyncio.wait_for(self.r.readline(), timeout=30.0)
            if not self.process_handshake(hs):
                return

            # start heartbeat handling now that the handshake is done
            self._last_message_time = time.monotonic()
            heartbeat_task = asyncio.async(self.handle_heartbeats())

            yield from self.handle_messages()

        except asyncio.IncompleteReadError:
            logging.info('Client EOF')

        except asyncio.CancelledError:
            logging.info('Client heartbeat timeout or other cancellation')

        except Exception:
            logging.exception('Exception handling client')

        finally:
            logging.info('Disconnected')

            self.send = self.write_discard  # suppress all output from hereon in

            if self._udp_key is not None:
                self.udp_protocol.remove_client(self._udp_key)

            # tell the coordinator, this might cause traffic to be suppressed
            # from other receivers
            if self.receiver is not None:
                self.coordinator.receiver_disconnect(self.receiver)

            if heartbeat_task is not None:
                heartbeat_task.cancel()
            if self._pending_flush is not None:
                self._pending_flush.cancel()
            if self._pending_traffic_update is not None:
                self._pending_traffic_update.cancel()

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
                for c, readmeth, writemeth in self.compression_methods:
                    if c in peer_compression_methods:
                        self.compress = c
                        self.handle_messages = readmeth
                        self.send = writemeth
                        break
                if self.compress is None:
                    raise ValueError('No mutually usable compression type')

                lat = float(hs['lat'])
                if lat < -90 or lat > 90:
                    raise ValueError('invalid latitude, should be -90 .. 90')

                lon = float(hs['lon'])
                if lon < -180 or lon > 360:
                    raise ValueError('invalid longitude, should be -180 .. 360')
                if lon > 180:
                    lon = lon - 180

                alt = float(hs['alt'])
                if alt < -1000 or alt > 10000:
                    raise ValueError('invalid altitude, should be -1000 .. 10000')

                ecef = geodesy.llh2ecef((lat, lon, alt))

                clock_type = str(hs.get('clock_type', 'dump1090'))
                user = str(hs['user'])

                if not hs.get('heartbeat', False):
                    raise ValueError('must use heartbeats')

                if not hs.get('selective_traffic', False):
                    raise ValueError('must use selective traffic')

                self.use_return_results = bool(hs.get('return_results', False))
                if self.use_return_results:
                    return_result_format = hs.get('return_result_format', 'old')
                    if return_result_format == 'old':
                        self.report_mlat_position = self.report_mlat_position_old
                    elif return_result_format == 'ecef':
                        self.report_mlat_position = self.report_mlat_position_ecef
                    else:
                        raise ValueError('invalid return_result_format, should be one of "old" or "ecef"')
                else:
                    self.report_mlat_position = self.report_mlat_position_discard

                self.use_udp = (self.udp_protocol is not None and bool(hs.get('udp_transport', False)))

                self.receiver = self.coordinator.new_receiver(connection=self,
                                                              user=user,
                                                              auth=hs.get('auth'),
                                                              clock_type=clock_type,
                                                              position=ecef)

            except KeyError as e:
                deny = 'Missing field in handshake: ' + str(e)

            except ValueError as e:
                deny = 'Bad values in handshake: ' + str(e)

        if deny:
            logging.info('Handshake failed: %s', deny)
            self.write_raw(deny=[deny], reconnect_in=util.fuzzy(900))
            return False

        # todo: MOTD
        response = {"compress": self.compress,
                    "reconnect_in": util.fuzzy(15),
                    "selective_traffic": True,
                    "heartbeat": True,
                    "return_results": self.use_return_results}

        if self.use_udp:
            self._udp_key = self.udp_protocol.add_client(sync_handler=self.process_sync,
                                                         mlat_handler=self.process_mlat)
            response['udp_transport'] = (None,   # use same host as TCP
                                         self.udp_protocol.listen_address[1],
                                         self._udp_key)

        self.write_raw(**response)
        return True

    def write_raw(self, **kwargs):
        line = json.dumps(kwargs) + '\n'
        self.w.write(line.encode('ascii'))

    def write_zlib(self, **kwargs):
        self._writebuf.append(json.dumps(kwargs) + '\n')
        if self._pending_flush is None:
            self._pending_flush = asyncio.get_event_loop().call_later(0.5, self._flush_zlib)

    def write_discard(self, **kwargs):
        pass

    def _flush_zlib(self):
        self._pending_flush = None

        if not self._writebuf:
            return

        if self._compressor is None:
            self._compressor = zlib.compressobj(1)

        data = bytearray(2)
        pending = False
        for line in self._writebuf:
            data += self._compressor.compress(line.encode('ascii'))
            pending = True

            if len(data) >= 32768:
                data += self._compressor.flush(zlib.Z_SYNC_FLUSH)
                #assert data[-4:] == b'\x00\x00\xff\xff'
                del data[-4:]
                assert len(data) < 65538
                data[0:2] = struct.pack('!H', len(data)-2)
                self.w.write(data)
                del data[2:]
                pending = False

        if pending:
            data += self._compressor.flush(zlib.Z_SYNC_FLUSH)
            #assert data[-4:] == b'\x00\x00\xff\xff'
            del data[-4:]
            assert len(data) < 65538
            data[0:2] = struct.pack('!H', len(data)-2)
            self.w.write(data)

        self._writebuf = []

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
        self._last_message_time = time.monotonic()
        msg = json.loads(line)

        if 'sync' in msg:
            sync = msg['sync']
            self.process_sync(float(sync['et']),
                              float(sync['ot']),
                              bytes.fromhex(sync['em']),
                              bytes.fromhex(sync['om']))
        elif 'mlat' in msg:
            mlat = msg['mlat']
            self.process_mlat(float(mlat['t']), bytes.fromhex(mlat['m']))
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
        elif 'rate_report' in msg:
            self.process_rate_report_message(msg['rate_report'])
        else:
            logging.info('Received an unexpected message: %s', msg)

    def process_sync(self, et, ot, em, om):
        self.coordinator.receiver_sync(self.receiver, et, ot, em, om)

    def process_mlat(self, t, m):
        self.coordinator.receiver_mlat(self.receiver, t, m)

    def process_seen_message(self, seen):
        seen = {int(icao, 16) for icao in seen}
        self.coordinator.receiver_tracking_add(self.receiver, seen)

    def process_lost_message(self, lost):
        lost = {int(icao, 16) for icao in lost}
        self.coordinator.receiver_tracking_remove(self.receiver, lost)

    def process_input_connected_message(self, m):
        self.coordinator.receiver_clock_reset(self.receiver)

    def process_input_disconnected_message(self, m):
        self.coordinator.receiver_clock_reset(self.receiver)

    def process_heartbeat_message(self, m):
        pass

    def process_rate_report_message(self, m):
        self.coordinator.receiver_rate_report(self.receiver, {int(k, 16): v for k, v in m.items()})

    # Connection interface

    # For traffic management, we update the local set and schedule a task to write it out in a little while.
    def request_traffic(self, receiver, icao):
        assert receiver is self.receiver

        if icao in self._wanted_traffic:
            return

        self._wanted_traffic.add(icao)
        if self._pending_traffic_update is None:
            self._pending_traffic_update = asyncio.get_event_loop().call_later(0.5, self.send_traffic_updates)

    def suppress_traffic(self, receiver, icao):
        assert receiver is self.receiver

        if icao not in self._wanted_traffic:
            return

        self._wanted_traffic.remove(icao)
        if self._pending_traffic_update is None:
            self._pending_traffic_update = asyncio.get_event_loop().call_later(0.5, self.send_traffic_updates)

    def send_traffic_updates(self):
        self._pending_traffic_update = None

        start_sending = self._wanted_traffic.difference(self._requested_traffic)
        if start_sending:
            self.send(start_sending=['{0:06x}'.format(i) for i in start_sending])

        stop_sending = self._requested_traffic.difference(self._wanted_traffic)
        if stop_sending:
            self.send(stop_sending=['{0:06x}'.format(i) for i in stop_sending])

        self._requested_traffic = set(self._wanted_traffic)

    # one of these is assigned to report_mlat_position:
    def report_mlat_position_discard(self, receiver,
                                     icao, utc, ecef, ecef_cov, nstations):
        # client is not interested
        pass

    def report_mlat_position_old(self, receiver,
                                 icao, utc, ecef, ecef_cov, nstations):
        # old client, use the old format (somewhat incomplete)
        lat, lon, alt = geodesy.ecef2llh(ecef)
        self.send(result={'@': round(utc, 3),
                          'addr': '{0:06x}'.format(icao),
                          'lat': round(lat, 4),
                          'lon': round(lon, 4),
                          'alt': round(alt * MTOF, 0),
                          'callsign': None,
                          'squawk': None,
                          'hdop': 0.0,
                          'vdop': 0.0,
                          'tdop': 0.0,
                          'gdop': 0.0,
                          'nstations': nstations})

    def report_mlat_position_ecef(self, receiver,
                                  icao, utc, ecef, ecef_cov, nstations):
        # newer client
        # ecef, cov rounded to ~10m precision
        # cov is just the upper triangular part of the covariance matrix;
        # the lower triangular part can be found by symmetry.
        self.send(result={'@': round(utc, 3),
                          'addr': '{0:06x}'.format(icao),
                          'ecef': (round(ecef[0], -1),
                                   round(ecef[1], -1),
                                   round(ecef[2], -1)),
                          'cov': (round(ecef_cov[0, 0], -2),
                                  round(ecef_cov[0, 1], -2),
                                  round(ecef_cov[0, 2], -2),
                                  round(ecef_cov[1, 1], -2),
                                  round(ecef_cov[1, 2], -2),
                                  round(ecef_cov[2, 2], -2)),
                          'nstat': nstations})
