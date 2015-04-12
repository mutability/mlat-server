# -*- mode: python; indent-tabs-mode: nil -*-


class TrackedAircraft(object):
    """A single tracked aircraft."""

    def __init__(self, icao):
        self.icao = icao
        self.tracked_by = set()
        self.requested_from = set()


class Tracker(object):
    """Tracks which receivers can see which aircraft, and asks receivers to
    forward traffic accordingly."""

    def __init__(self):
        self.aircraft = {}

    def add(self, receiver, icao_set):
        request_set = set()

        for icao in icao_set:
            ac = self.aircraft.get(icao)
            if ac is None:
                ac = self.aircraft[icao] = TrackedAircraft(icao)

            if len(ac.tracked_by) == 1:  # 1 -> 2 receivers
                request_set.add(icao)
                ac.requested_from.add(receiver)

                for other in ac.tracked_by:
                    other.connection.request_traffic(other, set([icao]))
                ac.requested_from.update(ac.tracked_by)

            ac.tracked_by.add(receiver)

        if request_set:
            receiver.connection.request_traffic(receiver, request_set)

    def remove(self, receiver, icao_set):
        for icao in icao_set:
            ac = self.aircraft.get(icao)
            if ac is not None:
                ac.tracked_by.remove(receiver)
                if len(ac.tracked_by) == 1:  # 2 -> 1 receivers
                    for other in ac.requested_from:
                        other.connection.suppress_traffic(other, set([icao]))
                    ac.requested_from.clear()

        receiver.connection.suppress_traffic(receiver, icao_set)

    def remove_all(self, receiver):
        icao_set = set()
        for icao, ac in self.aircraft.items():
            if receiver in ac.tracked_by:
                icao_set.add(icao)

        if len(icao_set) > 0:
            self.remove(receiver, icao_set)
