# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-server: a Mode S multilateration server
# Copyright (C) 2015  Oliver Jowett <oliver@mutability.co.uk>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Some common networking bits.
"""

import asyncio
import logging
import socket

from mlat.server import util


glogger = logging.getLogger("net")


class MonitoringListener(object):
    def __init__(self, host, port, factory, logger=glogger, description=None):
        if not description:
            description = self.__class__.__name__

        self.description = description
        self.logger = logger
        self.started = False
        self.host = host
        self.port = port
        self.factory = factory
        self.tcp_server = None
        self.clients = []
        self.monitoring = []

    @asyncio.coroutine
    def start(self):
        if not self.started:
            yield from self._start()
            self.started = True

        return self

    # override as needed:

    @asyncio.coroutine
    def _start(self):
        self.tcp_server = yield from asyncio.start_server(self.start_client,
                                                          host=self.host,
                                                          port=self.port)
        for s in self.tcp_server.sockets:
            name = s.getsockname()
            self.logger.info("{what} listening on {host}:{port} (TCP)".format(host=name[0],
                                                                              port=name[1],
                                                                              what=self.description))

    def _new_client(self, r, w):
        return self.factory(r, w)

    def _close(self):
        if self.tcp_server:
            self.tcp_server.close()
        for client in self.clients:
            client.close()
        self.clients.clear()

    # shouldn't need modifying:

    def start_client(self, r, w):
        newclient = self._new_client(r, w)
        self.clients.append(newclient)
        self.monitoring.append(asyncio.async(self.monitor_client(newclient)))

    @asyncio.coroutine
    def monitor_client(self, client):
        yield from client.wait_closed()
        self.clients.remove(client)
        self.monitoring.remove(asyncio.Task.current_task())

    def close(self):
        if not self.started:
            return

        self.started = False
        self._close()

        for m in self.monitoring:
            m.cancel()
        self.monitoring.clear()

    @asyncio.coroutine
    def wait_closed(self):
        yield from util.safe_wait(self.monitoring)
        if self.tcp_server:
            yield from self.tcp_server.wait_closed()


class MonitoringConnector(object):
    def __init__(self, host, port, reconnect_interval, factory):
        self.started = False
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval
        self.factory = factory
        self.reconnect_task = None
        self.client = None

    def start(self):
        if not self.started:
            self.started = True
            self.reconnect_task = asyncio.async(self.reconnect())

        return util.completed_future

    @asyncio.coroutine
    def reconnect(self):
        while True:
            try:
                reader, writer = yield from asyncio.open_connection(self.host, self.port)
            except socket.error:
                yield from asyncio.sleep(self.reconnect_interval)
                continue

            self.client = self.factory(reader, writer)
            yield from self.client.wait_closed()
            self.client = None
            yield from asyncio.sleep(self.reconnect_interval)

    def close(self):
        if not self.started:
            return

        self.started = False
        self.reconnect_task.cancel()
        if self.client:
            self.client.close()

    @asyncio.coroutine
    def wait_closed(self):
        yield from util.safe_wait([self.reconnect_task])
        if self.client:
            yield from self.client.wait_closed()
