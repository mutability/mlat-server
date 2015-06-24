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
Random utilities that don't fit elsewhere.
"""

import random
import asyncio
import logging


def fuzzy(t):
    return round(random.uniform(0.9*t, 1.1*t), 0)

completed_future = asyncio.Future()
completed_future.set_result(True)


def safe_wait(coros_or_futures, **kwargs):
    """Return a future that waits for all coroutines/futures in the given
    list to complete. Equivalent to asyncio.wait, except that the list may
    safely contain None (these values are ignored) or be entirely empty. If
    there is nothing to wait for, an already-completed future is returned."""

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


def setproctitle(title):
    """Set the process title. This implementation does nothing."""
    pass


try:
    # If the setproctitle module is available, use that.
    from setproctitle import setproctitle  # noqa
except ImportError:
    pass
