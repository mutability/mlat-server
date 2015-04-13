# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import bisect
import functools

from . import latlon
from . import clocksync
from . import cpr
from . import modes
from .constants import MAX_RANGE, FTOM, Cair


class SyncPoint(object):
    """A clock synchronization point."""
    def __init__(self, posA, posB, interval):
        self.posA = posA
        self.posB = posB
        self.interval = interval
        self.receivers = []


class ClockTracker(object):
    """Tracks clock state for receivers."""

    def __init__(self):
        self.sync_points = {}
        self.clock_pairs = {}

    def receiver_disconnect(self, receiver):
        # Clean up clock_pairs immediately.
        # Any membership in a pending sync point is noticed when the sync point is resolved.
        for k in list(self.sync_points.keys()):
            if k[0] is receiver or k[1] is receiver:
                del self.sync_points[k]

    def receiver_sync(self, receiver,
                      even_time, odd_time,
                      even_message, odd_message):
        # Do sanity checks.

        # Messages must be within 5 seconds of each other.
        if abs(even_time - odd_time) / receiver.clock.freq > 5.0:
            return

        # compute key and interval
        if even_time < odd_time:
            tA = even_time
            tB = odd_time
            key = even_message + odd_message
        else:
            tA = odd_time
            tB = even_time
            key = odd_message + even_message

        interval = (tB - tA) / receiver.clock.freq

        # do we have a suitable existing match?
        existing = self.sync_points.get(key)
        if existing:
            existing_intervals, existing_syncpoints = existing
            find = bisect.bisect_left(existing_intervals, interval)
            if find > 0 and (interval - existing_intervals[find - 1]) < 1e-3:
                # interval matches within 1ms, close enough.
                existing_syncpoints[find - 1].receivers.append((receiver, tA, tB))
                return

            if find < len(existing_intervals) and (existing_intervals[find] - interval) < 1e-3:
                # interval matches within 1ms, close enough.
                existing_syncpoints[find].receivers.append((receiver, tA, tB))
                return

        # No existing match. Validate the messages and maybe create a new sync point

        # basic validity
        even_message = modes.decode(even_message)
        if ((not even_message or
             even_message.df != 17 or
             not even_message.crc_ok or
             even_message.me.msgtype != modes.ESType.AIRBORNE_POSITION or
             even_message.me.fflag)):
            return

        odd_message = modes.decode(odd_message)
        if ((not odd_message or
             odd_message.df != 17 or
             not odd_message.crc_ok or
             odd_message.me.msgtype != modes.ESType.AIRBORNE_POSITION or
             odd_message.me.fflag)):
            return

        # quality checks
        if even_message.nuc < 7 or even_message.altitude is None:
            return

        if odd_message.nuc < 7 or odd_message.altitude is None:
            return

        if abs(even_message.altitude - odd_message.altitude) > 5000:
            return

        # find global positions
        even_lat, even_lon, odd_lat, odd_lon = cpr.decode(even_message.me.lat,
                                                          even_message.me.lon,
                                                          odd_message.me.lat,
                                                          odd_message.me.lon)
        if even_lat is None or odd_lat is None:
            return

        # range checks
        even_ecef = latlon.llh2ecef((even_lat,
                                     even_lon,
                                     even_message.altitude * FTOM))
        if latlon.ecef_distance(even_ecef, receiver.position) > MAX_RANGE:
            return

        odd_ecef = latlon.llh2ecef((odd_lat,
                                    odd_lon,
                                    odd_message.altitude * FTOM))
        if latlon.ecef_distance(odd_ecef, receiver.position) > MAX_RANGE:
            return

        if latlon.ecef_distance(even_ecef, odd_ecef) > 10000:
            return

        # valid. Create a new sync point.
        if even_time < odd_time:
            syncpoint = SyncPoint(even_ecef, odd_ecef, interval)
        else:
            syncpoint = SyncPoint(odd_ecef, even_ecef, interval)

        syncpoint.receivers.append((receiver, tA, tB))

        if existing:
            existing_intervals.insert(find, interval)
            existing_syncpoints.insert(find, syncpoint)
        else:
            self.sync_points[key] = ([interval], [syncpoint])

        asyncio.get_event_loop().call_later(
            2.0,
            functools.partial(self._resolve_syncpoint,
                              key=key,
                              interval=interval,
                              syncpoint=syncpoint))

    def _resolve_syncpoint(self, key, interval, syncpoint):
        # remove syncpoint from self.sync_points
        existing_intervals, existing_syncpoints = self.sync_points[key]
        find = bisect.bisect_left(existing_intervals, interval)
        assert existing_syncpoints[find] == syncpoint

        if len(existing_intervals) == 1:
            del self.sync_points[key]
        else:
            del existing_intervals[find]
            del existing_syncpoints[find]

        # process all pairs of receivers attached to the syncpoint
        n = len(syncpoint.receivers)
        for i in range(n):
            r0, t0A, t0B = syncpoint.receivers[i]
            if r0.dead:
                # receiver went away before we started resolving this
                continue

            for j in range(i+1, n):
                r1, t1A, t1B = syncpoint.receivers[j]
                if r1.dead:
                    # receiver went away before we started resolving this
                    continue

                if r0 is r1:
                    # odd, but could happen
                    continue

                # order the clockpair so that the first station always sorts lower
                if r0 < r1:
                    self._do_sync(syncpoint.posA, syncpoint.posB, r0, t0A, t0B, r1, t1A, t1B)
                else:
                    self._do_sync(syncpoint.posA, syncpoint.posB, r1, t1A, t1B, r0, t0A, t0B)

    def _do_sync(self, posA, posB, r0, t0A, t0B, r1, t1A, t1B):
        # find or create clock pair
        k = (r0, r1)
        pairing = self.clock_pairs.get(k)
        if pairing is None:
            pairing = clocksync.ClockPairing(r0.clock, r1.clock)
            self.clock_pairs[k] = pairing

        range0A = latlon.ecef_distance(posA, r0.position)
        range0B = latlon.ecef_distance(posB, r0.position)
        range1A = latlon.ecef_distance(posA, r1.position)
        range1B = latlon.ecef_distance(posB, r1.position)

        # propagation delays
        delay0A = range0A / Cair
        delay0B = range0B / Cair
        delay1A = range1A / Cair
        delay1B = range1B / Cair

        # compute intervals, adjusted for transmitter motion
        i0 = t0B - t0A + (delay0A - delay0B) * r0.clock.freq
        i1 = t1B - t1A + (delay1A - delay1B) * r1.clock.freq

        # do the update
        pairing.update(t0B, t1B, i0, i1)
