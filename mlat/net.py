# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import logging
import socket


glogger = logging.getLogger("net")


class MonitoringListener(object):
    def __init__(self, host, port, factory, *factory_args):
        self.started = False
        self.host = host
        self.port = port
        self.factory = factory
        self.factory_args = factory_args
        self.tcp_server = None
        self.clients = []
        self.monitoring = []

    @asyncio.coroutine
    def start(self):
        if not self.started:
            self.tcp_server = yield from asyncio.start_server(self.start_client,
                                                              host=self.host,
                                                              port=self.port)
            self.started = True

            for s in self.tcp_server.sockets:
                name = s.getsockname()
                glogger.info("Listening on {host}:{port}".format(host=name[0],
                                                                 port=name[1]))

        return self

    def start_client(self, r, w):
        newclient = self.factory(r, w, *self.factory_args)
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
        self.tcp_server.close()

        for m in self.monitoring:
            m.cancel()

    def wait_closed(self):
        return asyncio.wait([self.tcp_server.wait_closed()] + self.monitoring)


class MonitoringConnector(object):
    def __init__(self, host, port, reconnect_interval, factory, *factory_args):
        self.started = False
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval
        self.factory = factory
        self.factory_args = factory_args
        self.reconnect_task = None
        self.client = None

    # returns a future for consistency with MonitoringListener.start()
    def start(self):
        f = asyncio.Future()
        f.set_result(self)

        if self.started:
            return f

        self.started = True
        self.reconnect_task = asyncio.async(self.reconnect())
        return f

    @asyncio.coroutine
    def reconnect(self):
        while True:
            try:
                reader, writer = yield from asyncio.open_connection(self.host, self.port)
            except socket.error:
                yield from asyncio.sleep(self.reconnect_interval)
                continue

            self.client = self.factory(reader, writer, *self.factory_args)
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

    def wait_closed(self):
        waitlist = [self.reconnect_task]
        if self.client:
            waitlist.append(self.client.wait_closed())
        return asyncio.wait(waitlist)
