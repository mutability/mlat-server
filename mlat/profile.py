# -*- mode: python; indent-tabs-mode: nil -*-

# Part of mlat-client - an ADS-B multilateration client.
# Copyright 2015, Oliver Jowett <oliver@mutability.co.uk>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os

# NB: This requires Python 3.3 when MLAT_CPU_PROFILE is set.


if not int(os.environ.get('MLAT_CPU_PROFILE', '0')):
    enabled = False

    def trackcpu(f, **kwargs):
        return f

    def dump_cpu_profiles(tofile=None):
        pass
else:
    import sys
    import time
    import operator
    import functools

    _cpu_tracking = []
    print('CPU profiling enabled', file=sys.stderr)
    enabled = True
    baseline_cpu = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID)
    baseline_wall = time.monotonic()

    def trackcpu(f, name=None, **kwargs):
        if name is None:
            name = f.__module__ + '.' + f.__qualname__

        print('Profiling:', name, file=sys.stderr)
        tracking = [name, 0, 0.0]
        _cpu_tracking.append(tracking)

        @functools.wraps(f)
        def cpu_measurement_wrapper(*args, **kwargs):
            start = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID)
            try:
                return f(*args, **kwargs)
            finally:
                end = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID)
                tracking[1] += 1
                tracking[2] += (end - start)

        return cpu_measurement_wrapper

    def dump_cpu_profiles(tofile=sys.stderr):
        elapsed_cpu = time.clock_gettime(time.CLOCK_THREAD_CPUTIME_ID) - baseline_cpu
        elapsed_wall = time.monotonic() - baseline_wall

        print('Elapsed: {wall:.1f}   CPU: {cpu:.1f} ({percent:.0f}%)'.format(
            wall=elapsed_wall,
            cpu=elapsed_cpu,
            percent=100.0 * elapsed_cpu / elapsed_wall), file=tofile)
        print('{rank:4s} {name:60s} {count:6s} {persec:6s} {total:8s} {each:8s} {fraction:6s}'.format(
            rank='#',
            name='Function',
            count='Calls',
            persec='(/sec)',
            total='Total(s)',
            each='Each(us)',
            fraction="Frac"), file=tofile)

        rank = 1
        for name, count, total in sorted(_cpu_tracking, key=operator.itemgetter(2), reverse=True):
            if count == 0:
                break

            print('{rank:4d} {name:60s} {count:6d} {persec:6.1f} {total:8.3f} {each:8.0f} {fraction:6.1f}'.format(
                rank=rank,
                name=name,
                count=count,
                persec=1.0 * count / elapsed_wall,
                total=total,
                each=total * 1e6 / count,
                fraction=100.0 * total / elapsed_cpu), file=tofile)
            rank += 1

        tofile.flush()
