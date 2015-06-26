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
Manages the clock synchronization pairs between all receivers based on
DF17 position messages received by more than one receiver.
"""

__all__ = ('SyncPoint', 'ClockTracker')

import asyncio
import functools
import time
import logging

import modes.message

from mlat import geodesy, constants, profile
from mlat.server import clocksync, config


class SyncPoint(object):
    """A potential clock synchronization point.
    Clock synchronization points are a pair of DF17 messages,
    and associated timing info from all receivers that see
    that pair.
    """

    def __init__(self, address, posA, posB, interval):
        """Construct a new sync point.

        address: the ICAO address of the sync aircraft
        posA: the ECEF position of the earlier message
        posB: the ECEF position of the later message
        interval: the nominal interval (in seconds)
          between the two messages; this is as measured by
          the first receiver to report the pair, and is used
          to distinguish cases where the same message is
          transmitted more than once.
        """

        self.address = address
        self.posA = posA
        self.posB = posB
        self.interval = interval
        self.receivers = []  # a list of (receiver, timestampA, timestampB) values


class ClockTracker(object):
    """Maintains clock pairings between receivers, and matches up incoming sync messages
    from receivers to update the parameters of the pairings."""

    def __init__(self):
        # map of (sync key) -> list of sync points
        #
        # sync key is a pair of bytearrays: (msgA, msgB)
        # where msgA and msgB are the contents of the
        # earlier and later message of the pair respectively.
        self.sync_points = {}

        # map of (pair key) -> pairing
        #
        # pair key is (receiver 0, receiver 1) where receiver 0
        # is always less than receiver 1.
        self.clock_pairs = {}

        # schedule periodic cleanup
        asyncio.get_event_loop().call_later(1.0, self._cleanup)

    def _cleanup(self):
        """Called periodically to clean up clock pairings that have expired."""

        asyncio.get_event_loop().call_later(30.0, self._cleanup)

        now = time.monotonic()
        prune = set()
        for k, pairing in self.clock_pairs.items():
            if pairing.expiry <= now:
                prune.add(k)

        for k in prune:
            del self.clock_pairs[k]

    @profile.trackcpu
    def receiver_clock_reset(self, receiver):
        """
        Called by the coordinator when we should drop our clock sync
        state for a given receiver. This happens on input disconnect/
        reconnect.

        (This is actually the same work as receiver_disconnect for the moment)
        """
        for k in list(self.clock_pairs.keys()):
            if k[0] is receiver or k[1] is receiver:
                del self.clock_pairs[k]

    @profile.trackcpu
    def receiver_disconnect(self, receiver):
        """
        Called by the coordinator when a receiver disconnects.

        Clears up any clock pairing involving the receiver immediately,
        as it's very likely that any existing sync data will be invalid
        if/when the receiver later reconnects.

        Sync points involving the receiver are not cleaned up immediately.
        It's assumed that the disconnected receiver has the "dead" flag
        set; this flag is tested before sync happens.
        """

        # Clean up clock_pairs immediately.
        # Any membership in a pending sync point is noticed when we try to sync more receivers with it.
        for k in list(self.clock_pairs.keys()):
            if k[0] is receiver or k[1] is receiver:
                del self.clock_pairs[k]

    @profile.trackcpu
    def receiver_sync(self, receiver,
                      even_time, odd_time,
                      even_message, odd_message):
        """
        Called by the coordinator to handle a sync message from a receiver.

        Looks for a suitable existing sync point and, if there is one, does
        synchronization between this receiver and the existing receivers
        associated with the sync point.

        Otherwise, validates the message pair and, if it is suitable, creates a
        new sync point for it.

        receiver: the receiver reporting the sync message
        even_message: a DF17 airborne position message with F=0
        odd_message: a DF17 airborne position message with F=1
        even_time: the time of arrival of even_message, as seen by receiver.clock
        odd_time: the time of arrival of odd_message, as seen by receiver.clock
        """

        # Do sanity checks.

        # Messages must be within 5 seconds of each other.
        if abs(even_time - odd_time) / receiver.clock.freq > 5.0:
            return

        # compute key and interval
        if even_time < odd_time:
            tA = even_time
            tB = odd_time
            key = (even_message, odd_message)
        else:
            tA = odd_time
            tB = even_time
            key = (odd_message, even_message)

        interval = (tB - tA) / receiver.clock.freq

        # do we have a suitable existing match?
        syncpointlist = self.sync_points.get(key)
        if syncpointlist:
            for candidate in syncpointlist:
                if abs(candidate.interval - interval) < 1e-3:
                    # interval matches within 1ms, close enough.
                    self._add_to_existing_syncpoint(candidate, receiver, tA, tB)
                    return

        # No existing match. Validate the messages and maybe create a new sync point

        # basic validity
        even_message = modes.message.decode(even_message)
        if ((not even_message or
             even_message.DF != 17 or
             not even_message.crc_ok or
             even_message.estype != modes.message.ESType.airborne_position or
             even_message.F)):
            return

        odd_message = modes.message.decode(odd_message)
        if ((not odd_message or
             odd_message.DF != 17 or
             not odd_message.crc_ok or
             odd_message.estype != modes.message.ESType.airborne_position or
             not odd_message.F)):
            return

        if even_message.address != odd_message.address:
            return

        # quality checks
        if even_message.nuc < 6 or even_message.altitude is None:
            return

        if odd_message.nuc < 6 or odd_message.altitude is None:
            return

        if abs(even_message.altitude - odd_message.altitude) > 5000:
            return

        # find global positions
        try:
            even_lat, even_lon, odd_lat, odd_lon = modes.cpr.decode(even_message.LAT,
                                                                    even_message.LON,
                                                                    odd_message.LAT,
                                                                    odd_message.LON)
        except ValueError:
            # CPR failed
            return

        # convert to ECEF, do range checks
        even_ecef = geodesy.llh2ecef((even_lat,
                                      even_lon,
                                      even_message.altitude * constants.FTOM))
        if geodesy.ecef_distance(even_ecef, receiver.position) > config.MAX_RANGE:
            logging.info("{a:06X}: receiver range check (even) failed".format(a=even_message.address))
            return

        odd_ecef = geodesy.llh2ecef((odd_lat,
                                     odd_lon,
                                     odd_message.altitude * constants.FTOM))
        if geodesy.ecef_distance(odd_ecef, receiver.position) > config.MAX_RANGE:
            logging.info("{a:06X}: receiver range check (odd) failed".format(a=odd_message.address))
            return

        if geodesy.ecef_distance(even_ecef, odd_ecef) > config.MAX_INTERMESSAGE_RANGE:
            logging.info("{a:06X}: intermessage range check failed".format(a=even_message.address))
            return

        # valid. Create a new sync point.
        if even_time < odd_time:
            syncpoint = SyncPoint(even_message.address, even_ecef, odd_ecef, interval)
        else:
            syncpoint = SyncPoint(even_message.address, odd_ecef, even_ecef, interval)

        syncpoint.receivers.append([receiver, tA, tB, False])
        if not syncpointlist:
            syncpointlist = self.sync_points[key] = []
        syncpointlist.append(syncpoint)

        # schedule cleanup of the syncpoint after 2 seconds -
        # we should have seen all copies of those messages by
        # then.
        asyncio.get_event_loop().call_later(
            2.0,
            functools.partial(self._cleanup_syncpoint,
                              key=key,
                              syncpoint=syncpoint))

    def _add_to_existing_syncpoint(self, syncpoint, r0, t0A, t0B):
        # add a new receiver and timestamps to an existing syncpoint

        # new state for the syncpoint: receiver, timestamp A, timestamp B,
        # and a flag indicating if this receiver actually managed to sync
        # with another receiver using this syncpoint (used for stats)
        r0l = [r0, t0A, t0B, False]

        # try to sync the new receiver with all receivers that previously
        # saw the same pair
        for r1l in syncpoint.receivers:
            r1, t1A, t1B, r1sync = r1l

            if r1.dead:
                # receiver went away before we started resolving this
                continue

            if r0 is r1:
                # odd, but could happen
                continue

            # order the clockpair so that the receiver that sorts lower is the base clock
            if r0 < r1:
                if self._do_sync(syncpoint.address, syncpoint.posA, syncpoint.posB, r0, t0A, t0B, r1, t1A, t1B):
                    # sync worked, note it for stats
                    r0l[3] = r1l[3] = True
            else:
                if self._do_sync(syncpoint.address, syncpoint.posA, syncpoint.posB, r1, t1A, t1B, r0, t0A, t0B):
                    # sync worked, note it for stats
                    r0l[3] = r1l[3] = True

        # update syncpoint with the new receiver and we're done
        syncpoint.receivers.append(r0l)

    @profile.trackcpu
    def _cleanup_syncpoint(self, key, syncpoint):
        """Expire a syncpoint. This happens ~2 seconds after the first copy
        of a message pair is received.

        key: the key of the syncpoint
        syncpoint: the syncpoint itself
        """

        # remove syncpoint from self.sync_points, clean up empty entries
        l = self.sync_points[key]
        l.remove(syncpoint)
        if not l:
            del self.sync_points[key]

        # stats update
        for r, _, _, synced in syncpoint.receivers:
            if synced:
                r.sync_count += 1

    def _do_sync(self, address, posA, posB, r0, t0A, t0B, r1, t1A, t1B):
        # find or create clock pair
        k = (r0, r1)
        pairing = self.clock_pairs.get(k)
        if pairing is None:
            self.clock_pairs[k] = pairing = clocksync.ClockPairing(r0, r1)

        # propagation delays, in clock units
        delay0A = geodesy.ecef_distance(posA, r0.position) * r0.clock.freq / constants.Cair
        delay0B = geodesy.ecef_distance(posB, r0.position) * r0.clock.freq / constants.Cair
        delay1A = geodesy.ecef_distance(posA, r1.position) * r1.clock.freq / constants.Cair
        delay1B = geodesy.ecef_distance(posB, r1.position) * r1.clock.freq / constants.Cair

        # compute intervals, adjusted for transmitter motion
        i0 = (t0B - delay0B) - (t0A - delay0A)
        i1 = (t1B - delay1B) - (t1A - delay1A)

        if not pairing.is_new(t0B - delay0B):
            return True  # timestamp is in the past or duplicated, don't use this

        # do the update
        return pairing.update(address, t0B - delay0B, t1B - delay1B, i0, i1)

    def dump_receiver_state(self, receiver):
        state = {}
        for (r0, r1), pairing in self.clock_pairs.items():
            if pairing.n < 2:
                continue
            if r0 is receiver:
                state[r1.uuid] = [pairing.n,
                                  round(pairing.error * 1e6, 1),
                                  round(pairing.drift * 1e6, 2),
                                  pairing.ts_peer[-1] - pairing.ts_base[-1]]
            elif r1 is receiver:
                state[r0.uuid] = [pairing.n,
                                  round(pairing.error * 1e6, 1),
                                  round(pairing.i_drift * 1e6, 2),
                                  pairing.ts_base[-1] - pairing.ts_peer[-1]]
        return state
