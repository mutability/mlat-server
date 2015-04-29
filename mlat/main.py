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
Top level server object, arg parsing, etc.
"""

import asyncio
import logging
import signal
import argparse

import mlat.clientio
import mlat.coordinator
import mlat.net
import mlat.output

import mlat.leakcheck


def hostport(s):
    parts = s.split(':')
    if len(parts) != 2:
        raise argparse.ArgumentTypeError("{} should be in 'host:port' format".format(s))
    return (parts[0], int(parts[1]))


def port_or_hostport(s):
    parts = s.split(':')
    if len(parts) == 1:
        return ('0.0.0.0', int(parts[0]))
    if len(parts) == 2:
        return (parts[0], int(parts[1]))

    raise argparse.ArgumentTypeError("{} should be in 'port' or 'host:port' format".format(s))


def host_and_ports(s):
    try:
        parts = s.split(':')
        if len(parts) == 1:
            return (None, int(parts[0]), None)
        if len(parts) == 3:
            return (parts[0], int(parts[1]), int(parts[2]))
        if len(parts) != 2:
            raise ValueError()  # provoke ArgumentTypeError below

        # could be host:tcp_port or tcp_port:udp_port
        try:
            return (None, int(parts[0]), int(parts[1]))
        except ValueError:
            pass

        return (parts[0], int(parts[1]), None)
    except ValueError:
        raise argparse.ArgumentTypeError("{} should be in one of these formats: 'tcp_port', 'host:tcp_port', 'tcp_port:udp_port', 'host:tcp_port:udp_port'")  # noqa


class MlatServer(object):
    """The multilateration server.

    Derive from this if you want to add options, etc.
    """

    def __init__(self):
        self.loop = asyncio.get_event_loop()
        self.coordinator = None

    def make_arg_parser(self):
        parser = argparse.ArgumentParser(description="Multilateration server.")

        parser.add_argument('--client-listen',
                            help="listen on a [host:]tcp_port[:udp_port] for connections from multilateration clients. You should have at least one of these!",  # noqa
                            type=host_and_ports,
                            action='append',
                            required=True)
        parser.add_argument('--motd',
                            type=str,
                            help="set the server MOTD sent to clients.",
                            default="In-development v2 server. Expect odd behaviour.")

        parser.add_argument('--write-csv',
                            help="write results in CSV format to a local file.",
                            action='append',
                            default=[])

        parser.add_argument('--basestation-connect',
                            help="connect to a host:port and send Basestation-format results.",
                            action='append',
                            type=hostport,
                            default=[])
        parser.add_argument('--basestation-listen',
                            help="listen on a [host:]port and send Basestation-format results to clients that connect.",
                            action='append',
                            type=port_or_hostport,
                            default=[])

        parser.add_argument('--filtered-basestation-connect',
                            help="connect to a host:port and send Basestation-format results.",
                            action='append',
                            type=hostport,
                            default=[])
        parser.add_argument('--filtered-basestation-listen',
                            help="listen on a [host:]port and send Basestation-format results to clients that connect.",
                            action='append',
                            type=port_or_hostport,
                            default=[])

        parser.add_argument('--check-leaks',
                            help="run periodic memory leak checks (requires objgraph package).",
                            action='store_true',
                            default=False)

        parser.add_argument('--dump-pseudorange',
                            help="dump pseudorange data in json format to a file")

        return parser

    def make_subtasks(self, args):
        subtasks = [self.coordinator]

        if args.check_leaks:
            subtasks.append(mlat.leakcheck.LeakChecker())

        for host, tcp_port, udp_port in args.client_listen:
            subtasks.append(mlat.clientio.JsonClientListener(host=host,
                                                             tcp_port=tcp_port,
                                                             udp_port=udp_port,
                                                             coordinator=self.coordinator,
                                                             motd=args.motd))

        for host, port in args.basestation_connect:
            subtasks.append(mlat.output.make_basestation_connector(host=host,
                                                                   port=port,
                                                                   coordinator=self.coordinator,
                                                                   use_kalman_data=False))

        for host, port in args.basestation_listen:
            subtasks.append(mlat.output.make_basestation_listener(host=host,
                                                                  port=port,
                                                                  coordinator=self.coordinator,
                                                                  use_kalman_data=False))

        for host, port in args.filtered_basestation_connect:
            subtasks.append(mlat.output.make_basestation_connector(host=host,
                                                                   port=port,
                                                                   coordinator=self.coordinator,
                                                                   use_kalman_data=True))

        for host, port in args.filtered_basestation_listen:
            subtasks.append(mlat.output.make_basestation_listener(host=host,
                                                                  port=port,
                                                                  coordinator=self.coordinator,
                                                                  use_kalman_data=True))

        for filename in args.write_csv:
            subtasks.append(mlat.output.LocalCSVWriter(coordinator=self.coordinator,
                                                       filename=filename))

        return subtasks

    def stop(self, msg):
        logging.info(msg)
        self.loop.stop()

    def run(self):
        args = self.make_arg_parser().parse_args()

        self.coordinator = mlat.coordinator.Coordinator(pseudorange_filename=args.dump_pseudorange)

        subtasks = self.make_subtasks(args)

        # Start everything
        startup = asyncio.gather(*[x.start() for x in subtasks])
        self.loop.run_until_complete(startup)
        startup.result()  # provoke exceptions if something failed

        self.loop.add_signal_handler(signal.SIGINT, self.stop, "Halting on SIGINT")
        self.loop.add_signal_handler(signal.SIGTERM, self.stop, "Halting on SIGTERM")

        self.loop.run_forever()  # Well, until stop() is called anyway!

        logging.info("Server shutting down.")

        # Stop everything
        for t in reversed(subtasks):
            t.close()

        # Wait for completion
        shutdown = asyncio.gather(*[t.wait_closed() for t in subtasks], return_exceptions=True)
        self.loop.run_until_complete(shutdown)
        for e in shutdown.result():
            if isinstance(e, Exception) and not isinstance(e, asyncio.CancelledError):
                logging.error("Exception thrown during shutdown", exc_info=(type(e), e, e.__traceback__))

        self.loop.close()
        logging.info("Server shutdown done.")
