# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import logging
import time
import math
import functools
import socket

import mlat.constants
import mlat.geodesy

# various output methods for multilateration results


def format_time(timestamp):
    return time.strftime("%H:%M:%S", time.gmtime(timestamp)) + ".{0:03.0f}".format(math.modf(timestamp)[0] * 1000)


def format_date(timestamp):
    return time.strftime("%Y/%m/%d", time.gmtime(timestamp))


def csv_quote(s):
    if s is None:
        return ''
    if s.find('\n') == -1 and s.find('"') == -1 and s.find(',') == -1:
        return s
    else:
        return '"' + s.replace('"', '""') + '"'


class ConnectionLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return ('[{host}:{port}] {0}'.format(msg, **self.extra), kwargs)


class BasestationClient(object):
    TEMPLATE = 'MSG,{mtype},1,1,{addr:06X},1,{rcv_date},{rcv_time},{now_date},{now_time},{callsign},{altitude},{speed},{heading},{lat},{lon},{vrate},{squawk},{fs},{emerg},{ident},{aog}\n'  # noqa

    def __init__(self, reader, writer, *, coordinator, heartbeat_interval=30.0):
        peer = writer.get_extra_info('peername')
        self.host = peer[0]
        self.port = peer[1]
        self.logger = ConnectionLogger(logging.getLogger("basestation"), {'host': self.host, 'port': self.port})
        self.reader = reader
        self.writer = writer
        self.coordinator = coordinator
        self.heartbeat_interval = heartbeat_interval
        self.last_output = time.monotonic()
        self.heartbeat_task = asyncio.async(self.send_heartbeats())
        self.reader_task = asyncio.async(self.read_until_eof())

        self.logger.info("Connection established")
        self.coordinator.add_output_handler(self.write_result)

    def close(self):
        if not self.writer:
            return  # already closed

        self.logger.info("Connection lost")
        self.coordinator.remove_output_handler(self.write_result)
        self.heartbeat_task.cancel()
        self.writer.close()
        self.writer = None

    def wait_closed(self):
        return asyncio.wait([self.heartbeat_task, self.reader_task])

    @asyncio.coroutine
    def read_until_eof(self):
        try:
            while True:
                r = yield from self.reader.read(1024)
                if len(r) == 0:
                    self.logger.info("Client EOF")
                    # EOF
                    self.close()
                    return
        except socket.error:
            self.close()
            return

    @asyncio.coroutine
    def send_heartbeats(self):
        try:
            while True:
                now = time.monotonic()
                delay = self.last_output + self.heartbeat_interval - now
                if delay > 0.1:
                    yield from asyncio.sleep(delay)
                    continue

                self.writer.write(b'\n')
                self.last_output = now

        except socket.error:
            self.close()
            return

    def write_result(self, receive_timestamp, address, ecef, ecef_cov, receivers, distinct):
        try:
            lat, lon, alt = mlat.geodesy.ecef2llh(ecef)

            ac = self.coordinator.tracker.aircraft[address]
            callsign = ac.callsign
            squawk = ac.squawk
            altitude = int(round(alt * mlat.constants.MTOF))
            send_timestamp = time.time()

            line = self.TEMPLATE.format(mtype=3,
                                        addr=address,
                                        rcv_date=format_date(receive_timestamp),
                                        rcv_time=format_time(receive_timestamp),
                                        now_date=format_date(send_timestamp),
                                        now_time=format_time(send_timestamp),
                                        callsign=csv_quote(callsign),
                                        squawk=csv_quote(squawk),
                                        lat=round(lat, 4),
                                        lon=round(lon, 4),
                                        altitude=altitude,
                                        speed='',
                                        heading='',
                                        vrate='',
                                        fs='',
                                        emerg='',
                                        ident='',
                                        aog='')
            self.writer.write(line.encode('ascii'))
            self.last_output = time.monotonic()

        except Exception:
            self.logger.exception("Failed to write result")
            # swallow the exception so we don't affect our caller


def make_basestation_listener(host, port, coordinator):
    return mlat.net.MonitoringListener(host, port,
                                       functools.partial(BasestationClient,
                                                         coordinator=coordinator))


def make_basestation_connector(host, port, coordinator):
    return mlat.net.MonitoringConnector(host, port, 30.0,
                                        functools.partial(BasestationClient,
                                                          coordinator=coordinator))
