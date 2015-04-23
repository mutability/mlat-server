# -*- mode: python; indent-tabs-mode: nil -*-

# uses objgraph (and adapts some code from it), which has licence:

# Copyright (c) 2008-2015 Marius Gedminas <marius@pov.lt> and contributors
# Released under the MIT licence.

# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import asyncio
import gc
import logging
import operator

glogger = logging.getLogger("leaks")


def check_leaks(peak, suppress=False, limit=20):
    stats = objgraph.typestats(shortnames=False)
    deltas = {}
    for name, count in stats.items():
        old_count = peak.get(name, 0)
        if count > old_count:
            deltas[name] = count - old_count
            peak[name] = count

    deltas = sorted(deltas.items(), key=operator.itemgetter(1), reverse=True)
    deltas = deltas[:limit]

    if not suppress:
        if deltas:
            glogger.info("Peak memory usage change:")
            width = max(len(name) for name, count in deltas)
            for name, delta in deltas:
                glogger.info('  %-*s%9d %+9d' % (width, name, stats[name], delta))


def show_hogs(limit=20):
    glogger.info("Top memory hogs:")
    stats = objgraph.most_common_types(limit=limit, shortnames=False)
    width = max(len(name) for name, count in stats)
    for name, count in stats:
        glogger.info('  %-*s %i' % (width, name, count))


@asyncio.coroutine
def leak_checker():
    yield from asyncio.sleep(120.0)  # let startup settle

    peak = {}
    gc.collect()
    check_leaks(peak, suppress=True)
    while True:
        try:
            gc.collect()
            show_hogs()
            check_leaks(peak)
        except Exception:
            glogger.exception("leak checking failed")

        yield from asyncio.sleep(3600.0)


try:
    import objgraph

    def start_leak_checks():
        asyncio.async(leak_checker())

except ImportError:
    def start_leak_checks():
        glogger.warning("Leak checking disabled (objgraph not available)")
