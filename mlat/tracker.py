# -*- mode: python; indent-tabs-mode: nil -*-

import logging


class TrackedAircraft(object):
    """A single tracked aircraft."""

    def __init__(self, icao):
        # ICAO address of this aircraft
        self.icao = icao

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

    @property
    def interesting(self):
        """Is this aircraft interesting, i.e. should we forward traffic for it?"""
        return bool(self.sync_interest or len(self.mlat_interest) >= 3)

    def __lt__(self, other):
        return self.icao < other.icao


class Tracker(object):
    """Tracks which receivers can see which aircraft, and asks receivers to
    forward traffic accordingly."""

    def __init__(self):
        self.aircraft = {}

    def add(self, receiver, icao_set):
        for icao in icao_set:
            ac = self.aircraft.get(icao)
            if ac is None:
                ac = self.aircraft[icao] = TrackedAircraft(icao)

            ac.tracking.add(receiver)
            receiver.tracking.add(ac)

    def remove(self, receiver, icao_set):
        for icao in icao_set:
            ac = self.aircraft[icao]
            ac.tracking.remove(receiver)
            receiver.tracking.remove(ac)

    def remove_all(self, receiver):
        for icao in receiver.tracking:
            ac = self.aircraft.get(icao)
            if ac:
                ac.tracking.discard(receiver)
            receiver.connection.suppress_traffic(receiver, icao)

        receiver.tracking.clear()
        self._update_interest_sets(receiver, set(), set())

    def update_interest(self, receiver):
        """Update the interest sets of one receiver based on the
        latest tracking and rate report data."""

        if receiver.last_rate_report is None:
            # Legacy client, no rate report, we cannot be very selective.
            new_sync = {ac for ac in receiver.tracking if len(ac.tracking) > 1}
            new_mlat = receiver.tracking.copy()

            self._update_interest_sets(receiver, new_sync, new_mlat)
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
            if ac.icao not in receiver.last_rate_report:
                new_mlat_set.add(ac)

        logging.info("%s recalculated from rate reports", receiver.user)
        logging.info("   sync: %s", ','.join(['{0:06X}'.format(x.icao) for x in new_sync_set]))
        logging.info("   mlat: %s", ','.join(['{0:06X}'.format(x.icao) for x in new_mlat_set]))

        self._update_interest_sets(receiver, new_sync_set, new_mlat_set)

    def _update_interest_sets(self, receiver, new_sync_interest, new_mlat_interest):
        """
        Update the interest sets for a receiver, and
        the corresponding sets of the
        aircraft based on the change to the interest
        set.

        If the change to an interest set of an
        aircraft makes the set go from not-interesting
        to interesting, or vice versa, do the appropriate
        callbacks to all receivers tracking the aircraft
        to request or suppress traffic for the aircraft.

        receiver: the receiver to update
        new_sync_set:  the new sync-interest set
        new_mlat_set:  the new mlat-interest set
        """

        #
        # Sync interest set
        #

        for added in new_sync_interest.difference(receiver.sync_interest):
            was_interesting = added.interesting
            added.sync_interest.add(receiver)
            if added.interesting and not was_interesting:
                # Request traffic for this aircraft.
                for other_receiver in added.tracking:
                    other_receiver.connection.request_traffic(other_receiver, added.icao)

        for removed in receiver.sync_interest.difference(new_sync_interest):
            was_interesting = removed.interesting
            removed.sync_interest.remove(receiver)
            if was_interesting and not removed.interesting:
                # Suppress traffic for this aircraft.
                for other_receiver in removed.tracking:
                    other_receiver.connection.suppress_traffic(other_receiver, removed.icao)

        receiver.sync_interest = new_sync_interest

        #
        # Mlat interest set
        #

        for added in new_mlat_interest.difference(receiver.mlat_interest):
            was_interesting = added.interesting
            added.mlat_interest.add(receiver)
            if added.interesting and not was_interesting:
                # Request traffic for this aircraft.
                for other_receiver in added.tracking:
                    other_receiver.connection.request_traffic(other_receiver, added.icao)

        for removed in receiver.mlat_interest.difference(new_mlat_interest):
            was_interesting = removed.interesting
            removed.mlat_interest.remove(receiver)
            if was_interesting and not removed.interesting:
                # Suppress traffic for this aircraft.
                for other_receiver in removed.tracking:
                    other_receiver.connection.suppress_traffic(other_receiver, removed.icao)

        receiver.mlat_interest = new_mlat_interest
