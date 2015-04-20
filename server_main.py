#!/usr/bin/python3

import asyncio
import logging
import signal

import mlat.clientio
import mlat.coordinator


def stop_event_loop(msg, loop):
    logging.info(msg)
    loop.stop()


def main(tcp_port, udp_port):
    loop = asyncio.get_event_loop()

    coordinator = mlat.coordinator.Coordinator()

    start_task = mlat.clientio.start_listeners(tcp_port, udp_port, coordinator)
    tcp_server, udp_transport = loop.run_until_complete(start_task)

    try:
        print('Serving on {}'.format(tcp_server.sockets[0].getsockname()))
        if udp_transport:
            print('UDP listening on {}'.format(udp_transport.get_extra_info('sockname')))

        loop.add_signal_handler(signal.SIGINT, stop_event_loop, "Halting on SIGINT", loop)
        loop.add_signal_handler(signal.SIGTERM, stop_event_loop, "Halting on SIGTERM", loop)

        loop.run_forever()  # Well, until stop() is called anyway!

    finally:
        if udp_transport:
            udp_transport.abort()

        tcp_server.close()
        loop.run_until_complete(tcp_server.wait_closed())

        loop.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        style='{',
                        format='{asctime}.{msecs:03.0f}  {levelname:8s} {name:20s} {message}',
                        datefmt='%Y%m%d %H:%M:%S')
    main(40147, None)
