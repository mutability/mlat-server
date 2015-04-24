# -*- mode: python; indent-tabs-mode: nil -*-

import random
import asyncio
import logging


def fuzzy(t):
    return round(random.uniform(0.9*t, 1.1*t), 0)

completed_future = asyncio.Future()
completed_future.set_result(True)


def safe_wait(coros_or_futures, **kwargs):
    l = []
    for coro_or_future in coros_or_futures:
        if coro_or_future is not None:
            l.append(coro_or_future)

    if l:
        return asyncio.wait(l, **kwargs)
    else:
        return completed_future


class TaggingLogger(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'tag' in self.extra:
            return ('[{tag}] {0}'.format(msg, **self.extra), kwargs)
        else:
            return (msg, kwargs)
