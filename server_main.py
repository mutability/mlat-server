#!/usr/bin/env python3.4

import asyncio
import logging
import signal
import argparse

import mlat.clientio
import mlat.coordinator
import mlat.net
import mlat.output


def stop_event_loop(msg, loop):
    logging.info(msg)
    loop.stop()


def main(tcp_port, udp_port, motd, bind_address, basestation_connect, basestation_listen, csv_files):
    loop = asyncio.get_event_loop()

    csv_file_handlers = []
    net_handlers = []

    coordinator = mlat.coordinator.Coordinator()
    server = loop.run_until_complete(mlat.clientio.start_client_listener(tcp_port=tcp_port,
                                                                         udp_port=udp_port,
                                                                         coordinator=coordinator,
                                                                         motd=motd,
                                                                         bind_address=bind_address))

    for host, port in basestation_connect:
        net_handlers.append(mlat.output.make_basestation_connector(host=host,
                                                                   port=port,
                                                                   coordinator=coordinator))

    for host, port in basestation_listen:
        net_handlers.append(mlat.output.make_basestation_listener(host=host,
                                                                  port=port,
                                                                  coordinator=coordinator))

    for filename in csv_files:
        csv_file_handlers.append(mlat.output.LocalCSVWriter(coordinator=coordinator,
                                                            filename=filename))

    if net_handlers:
        loop.run_until_complete(asyncio.wait([x.start() for x in net_handlers]))

    #loop.add_signal_handler(signal.SIGINT, stop_event_loop, "Halting on SIGINT", loop)
    loop.add_signal_handler(signal.SIGTERM, stop_event_loop, "Halting on SIGTERM", loop)

    try:
        loop.run_forever()  # Well, until stop() is called anyway!

    finally:
        for h in csv_file_handlers:
            h.close()
        for h in net_handlers:
            h.close()
        server.close()
        coordinator.close()

        waitlist = [h.wait_closed() for h in net_handlers] + [server.wait_closed(), coordinator.wait_closed()]
        loop.run_until_complete(asyncio.wait(waitlist))
        loop.close()


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


def argparser():
    parser = argparse.ArgumentParser(description="Multilateration server.")

    parser.add_argument('--tcp-port',
                        help="Port to accept TCP control connections on.",
                        type=int,
                        required=True)
    parser.add_argument('--udp-port',
                        help="Port to accept UDP datagram traffic on.",
                        type=int)
    parser.add_argument('--motd',
                        type=str,
                        help="Server MOTD",
                        default="In-development v2 server. Expect odd behaviour.")
    parser.add_argument('--bind-address',
                        help="Host to bind to when accepting connections.",
                        default="0.0.0.0")

    parser.add_argument('--write-csv',
                        help="CSV file path to write results to",
                        action='append',
                        default=[])

    parser.add_argument('--basestation-connect',
                        help="Connect to a host:port and send Basestation-format output there",
                        action='append',
                        type=hostport,
                        default=[])
    parser.add_argument('--basestation-listen',
                        help="Listen on a [host:]port and send Basestation-format output to clients that connect",
                        action='append',
                        type=port_or_hostport,
                        default=[])

    return parser

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        style='{',
                        format='{asctime}.{msecs:03.0f}  {levelname:8s} {name:20s} {message}',
                        datefmt='%Y%m%d %H:%M:%S')

    args = argparser().parse_args()

    main(tcp_port=args.tcp_port,
         udp_port=args.udp_port,
         bind_address=args.bind_address,
         basestation_connect=args.basestation_connect,
         basestation_listen=args.basestation_listen,
         csv_files=args.write_csv,
         motd=args.motd)
