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


# Derived from (and uses) objgraph, which has licence:

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

"""
Simple periodic memory leak checker.
"""

import asyncio
import gc
import logging
import operator

from mlat.server import util

try:
    import objgraph
except ImportError:
    objgraph = None


class LeakChecker(object):
    def __init__(self):
        self.logger = logging.getLogger("leaks")
        self._task = None
        self.peak = {}

    def start(self):
        if objgraph is None:
            self.logger.warning("Leak checking disabled (objgraph not available)")
        else:
            self._task = asyncio.async(self.checker())

        return util.completed_future

    def close(self):
        if self._task:
            self._task.cancel()

    @asyncio.coroutine
    def wait_closed(self):
        yield from util.safe_wait([self._task])

    @asyncio.coroutine
    def checker(self):
        yield from asyncio.sleep(120.0)  # let startup settle

        gc.collect()
        self.check_leaks(suppress=True)

        while True:
            yield from asyncio.sleep(3600.0)

            try:
                gc.collect()
                self.show_hogs()
                self.check_leaks()
            except Exception:
                self.logger.exception("leak checking failed")

    def check_leaks(self, suppress=False, limit=20):
        stats = objgraph.typestats(shortnames=False)
        deltas = {}
        for name, count in stats.items():
            old_count = self.peak.get(name, 0)
            if count > old_count:
                deltas[name] = count - old_count
                self.peak[name] = count

        deltas = sorted(deltas.items(), key=operator.itemgetter(1), reverse=True)
        deltas = deltas[:limit]

        if not suppress:
            if deltas:
                self.logger.info("Peak memory usage change:")
                width = max(len(name) for name, count in deltas)
                for name, delta in deltas:
                    self.logger.info('  %-*s%9d %+9d' % (width, name, stats[name], delta))

    def show_hogs(self, limit=20):
        self.logger.info("Top memory hogs:")
        stats = objgraph.most_common_types(limit=limit, shortnames=False)
        width = max(len(name) for name, count in stats)
        for name, count in stats:
            self.logger.info('  %-*s %i' % (width, name, count))
