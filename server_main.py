#!/usr/bin/python3

import asyncio
import logging

from mlat import clientio


def main(port):
    loop = asyncio.get_event_loop()
    server_task = asyncio.start_server(clientio.start_client, port=port)
    server = loop.run_until_complete(server_task)

    print('Serving on {}'.format(server.sockets[0].getsockname()))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())

    loop.close()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,
                        style='{',
                        format='{asctime}.{msecs:03.0f}  {levelname:8s} {name:20s} {message}',
                        datefmt='%Y%m%d %H:%M:%S')
    main(12345)
