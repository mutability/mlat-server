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
Maintains state for all aircraft known to some client.
Works out the set of "interesting" aircraft and arranges for clients to
send us traffic for these.
"""

import asyncio
from mlat import profile
from mlat.server import kalman


class TrackedAircraft(object):
    """A single tracked aircraft."""

    def __init__(self, icao, allow_mlat):
        # ICAO address of this aircraft
        self.icao = icao

        # Allow mlat of this aircraft?
        self.allow_mlat = allow_mlat

        # set of receivers that can see this aircraft.
        # invariant: r.tracking.contains(a) iff a.tracking.contains(r)
        self.tracking = set()

        # set of receivers who want to use this aircraft for synchronization.
        # this aircraft is interesting if this set is non-empty.
        # invariant: r.sync_interest.contains(a) iff a.sync_interest.contains(r)
        self.sync_interest = set()

        # set of receivers who want to use this aircraft for multilateration.
        # this aircraft is interesting if this set has at least three receivers.
        # invariant: r.mlat_interest.contains(a) iff a.mlat_interest.contains(r)
        self.mlat_interest = set()

        # set of receivers that have contributed to at least one multilateration
        # result. This is used to decide who to forward results to.
        self.successful_mlat = set()

        # number of mlat message resolves attempted
        self.mlat_message_count = 0
        # number of mlat messages that produced valid least-squares results
        self.mlat_result_count = 0
        # number of mlat messages that produced valid kalman state updates
        self.mlat_kalman_count = 0

        # last reported altitude (for multilaterated aircraft)
        self.altitude = None
        # time of last altitude (time.monotonic())
        self.last_altitude_time = None

        # last multilateration, time (monotonic)
        self.last_result_time = None
        # last multilateration, ECEF position
        self.last_result_position = None
        # last multilateration, variance
        self.last_result_var = None
        # last multilateration, distinct receivers
        self.last_result_distinct = None
        # kalman filter state
        self.kalman = kalman.KalmanStateCA(self.icao)

        self.callsign = None
        self.squawk = None

    @property
    def interesting(self):
        """Is this aircraft interesting, i.e. should we forward traffic for it?"""
        return bool(self.sync_interest or (self.allow_mlat and len(self.mlat_interest) >= 3))

    def __lt__(self, other):
        return self.icao < other.icao


class Tracker(object):
    """Tracks which receivers can see which aircraft, and asks receivers to
    forward traffic accordingly."""

    def __init__(self, partition):
        self.aircraft = {}
        self.partition_id = partition[0] - 1
        self.partition_count = partition[1]

    def in_local_partition(self, icao):
        if self.partition_count == 1:
            return True

        # mix the address a bit
        h = icao
        h = (((h >> 16) ^ h) * 0x45d9f3b) & 0xFFFFFFFF
        h = (((h >> 16) ^ h) * 0x45d9f3b) & 0xFFFFFFFF
        h = ((h >> 16) ^ h)
        return bool((h % self.partition_count) == self.partition_id)

    def add(self, receiver, icao_set):
        for icao in icao_set:
            ac = self.aircraft.get(icao)
            if ac is None:
                ac = self.aircraft[icao] = TrackedAircraft(icao, self.in_local_partition(icao))

            ac.tracking.add(receiver)
            receiver.tracking.add(ac)

    def remove(self, receiver, icao_set):
        for icao in icao_set:
            ac = self.aircraft.get(icao)
            if not ac:
                continue

            ac.tracking.discard(receiver)
            ac.successful_mlat.discard(receiver)
            receiver.tracking.discard(ac)
            if not ac.tracking:
                del self.aircraft[icao]

    def remove_all(self, receiver):
        for ac in receiver.tracking:
            ac.tracking.discard(receiver)
            ac.successful_mlat.discard(receiver)
            ac.sync_interest.discard(receiver)
            ac.mlat_interest.discard(receiver)
            if not ac.tracking:
                del self.aircraft[ac.icao]

        receiver.tracking.clear()
        receiver.sync_interest.clear()
        receiver.mlat_interest.clear()

    @profile.trackcpu
    def update_interest(self, receiver):
        """Update the interest sets of one receiver based on the
        latest tracking and rate report data."""

        if receiver.last_rate_report is None:
            # Legacy client, no rate report, we cannot be very selective.
            new_sync = {ac for ac in receiver.tracking if len(ac.tracking) > 1}
            new_mlat = {ac for ac in receiver.tracking if ac.allow_mlat}
            receiver.update_interest_sets(new_sync, new_mlat)
            asyncio.get_event_loop().call_later(15.0, receiver.refresh_traffic_requests)
            return

        # Work out the aircraft that are transmitting ADS-B that this
        # receiver wants to use for synchronization.
        ac_to_ratepair_map = {}
        ratepair_list = []
        for icao, rate in receiver.last_rate_report.items():
            if rate < 0.20:
                continue

            ac = self.aircraft.get(icao)
            if not ac:
                continue

            ac_to_ratepair_map[ac] = l = []  # list of (rateproduct, receiver, ac) tuples for this aircraft
            for r1 in ac.tracking:
                if receiver is r1:
                    continue

                if r1.last_rate_report is None:
                    # Receiver that does not produce rate reports, just take a guess.
                    rate1 = 1.0
                else:
                    rate1 = r1.last_rate_report.get(icao, 0.0)

                rp = rate * rate1 / 4.0
                if rp < 0.10:
                    continue

                ratepair = (rp, r1, ac)
                l.append(ratepair)
                ratepair_list.append(ratepair)

        ratepair_list.sort()

        ntotal = {}
        new_sync_set = set()
        for rp, r1, ac in ratepair_list:
            if ac in new_sync_set:
                continue  # already added

            if ntotal.get(r1, 0.0) < 1.0:
                # use this aircraft for sync
                new_sync_set.add(ac)
                # update rate-product totals for all receivers that see this aircraft
                for rp2, r2, ac2 in ac_to_ratepair_map[ac]:
                    ntotal[r2] = ntotal.get(r2, 0.0) + rp2

        # for multilateration we are interesting in
        # all aircraft that we are tracking but for
        # which we have no ADS-B rate (i.e. are not
        # transmitting positions)
        new_mlat_set = set()
        for ac in receiver.tracking:
            if ac.icao not in receiver.last_rate_report and ac.allow_mlat:
                new_mlat_set.add(ac)

        receiver.update_interest_sets(new_sync_set, new_mlat_set)
        asyncio.get_event_loop().call_later(15.0, receiver.refresh_traffic_requests)
