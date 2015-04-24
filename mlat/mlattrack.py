# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import time
import logging
import operator
import numpy

import modes.message
import mlat.config
import mlat.clocknorm
import mlat.solver
import mlat.geodesy
import mlat.constants

glogger = logging.getLogger("mlattrack")


class MessageGroup:
    def __init__(self, first_seen, message):
        self.message = message
        self.first_seen = first_seen
        self.copies = []
        self.handle = None


class MlatTracker(object):
    def __init__(self, coordinator):
        self.pending = {}
        self.coordinator = coordinator
        self.tracker = coordinator.tracker
        self.clock_tracker = coordinator.clock_tracker

    def receiver_mlat(self, receiver, timestamp, message):
        # use message as key
        group = self.pending.get(message)
        if not group:
            group = self.pending[message] = MessageGroup(time.time(), message)
            group.handle = asyncio.get_event_loop().call_later(
                mlat.config.MLAT_DELAY,
                self._resolve,
                group)

        group.copies.append((receiver, timestamp))

    def _resolve(self, group):
        del self.pending[group.message]

        # less than 3 messages -> no go
        if len(group.copies) < 3:
            return

        decoded = modes.message.decode(group.message)

        ac = self.tracker.aircraft.get(decoded.address)
        if not ac:
            return

        # When we've seen a few copies of the same message, it's
        # probably correct. Update the tracker with newly seen
        # altitudes, squawks, callsigns.
        if decoded.altitude is not None:
            ac.altitude = decoded.altitude
            ac.last_altitude_time = time.monotonic()

        if decoded.squawk is not None:
            ac.squawk = decoded.squawk

        if decoded.callsign is not None:
            ac.callsign = decoded.callsign

        if ac.last_position_time is not None and time.monotonic() - ac.last_position_time < 2.0:
            return  # ratelimit to one position per 2 seconds

        # construct a map of receiver -> list of timestamps
        timestamp_map = {}
        for receiver, timestamp in group.copies:
            timestamp_map.setdefault(receiver, []).append(timestamp)

        # need 3 separate receivers at a bare minimum for multilateration
        if len(timestamp_map) < 3:
            return

        # normalize timestamps. This returns a list of timestamp maps;
        # within each map, the timestamp values are comparable to each other.
        components = mlat.clocknorm.normalize(clocktracker=self.clock_tracker,
                                              timestamp_map=timestamp_map)

        # cluster timestamps into clusters that are probably copies of the
        # same transmission.
        clusters = []
        for component in components:
            if len(component) >= 3:  # don't bother with orphan components at all
                clusters.extend(_cluster_timestamps(component))

        if not clusters:
            return

        # find altitude
        if decoded.altitude is not None:
            altitude = decoded.altitude
        else:
            if ac.altitude is None:
                return
            if time.monotonic() - ac.last_altitude_time > 30.0:
                return
            altitude = ac.altitude

        # Convert to meters
        altitude = altitude * mlat.constants.FTOM

        # If we have a recent position use that as the starting point for the solver.
        # Otherwise, we will fall back to using the closest station.
        if ac.position is None or time.monotonic() > ac.last_position_time > 120:
            position = None
        else:
            position = ac.position

        # start from the largest cluster
        result = None
        clusters.sort(key=operator.itemgetter(0))
        while clusters and not result:
            distinct, cluster = clusters.pop()
            cluster.sort(key=operator.itemgetter(1))  # sort by increasing timestamp (todo: just assume descending..)
            r = mlat.solver.solve(cluster, altitude, position if position else cluster[0][0].position)
            if r:
                # estimate the error
                ecef, ecef_cov = r
                if ecef_cov is not None:
                    var_est = numpy.sum(numpy.diagonal(ecef_cov))
                else:
                    var_est = 0

                if var_est < 100e6:
                    result = r

        if not result:
            return

        ecef, ecef_cov = result
        ac.position = ecef
        ac.last_position_time = time.monotonic()

        #lat, lon, alt = mlat.geodesy.ecef2llh(ecef)
        #glogger.info("Success! {a:06X} at {lat:.4f},{lon:.4f},{alt:.0f}  ({d}/{n} stations, {err:.0f}m error)".format(
        #    a=decoded.address,
        #    lat=lat,
        #    lon=lon,
        #    alt=alt*mlat.constants.MTOF,
        #    n=len(cluster),
        #    d=distinct,
        #    err=math.sqrt(var_est)))

        for handler in self.coordinator.output_handlers:
            handler(group.first_seen, decoded.address,
                    ecef, ecef_cov,
                    [receiver for receiver, timestamp, error in cluster], distinct)


def _cluster_timestamps(component):
    #glogger.info("cluster these:")

    # flatten the component into a list of tuples
    flat_component = []
    for receiver, (error, timestamps) in component.items():
        for timestamp in timestamps:
            #glogger.info("  {r} {t:.1f}us {e:.1f}us".format(r=receiver.user, t=timestamp*1e6, e=error*1e6))
            flat_component.append((receiver, timestamp, error))

    # sort by timestamp
    flat_component.sort(key=operator.itemgetter(1))

    # do a rough clustering: groups of items with inter-item spacing of less than 2ms
    group = [flat_component[0]]
    groups = [group]
    for t in flat_component[1:]:
        if (t[1] - group[-1][1]) > 2e-3:
            group = [t]
            groups.append(group)
        else:
            group.append(t)

    # inspect each group and produce clusters
    # this is about O(n^2)-ish with group size, which
    # is why we try to break up the component into
    # smaller groups first.

    #glogger.info("{n} groups".format(n=len(groups)))

    clusters = []
    for group in groups:
        #glogger.info(" group:")
        #for r, t, e in group:
        #    glogger.info("  {r} {t:.1f}us {e:.1f}us".format(r=r.user, t=t*1e6, e=e*1e6))

        while len(group) >= 3:
            tail = group.pop()
            cluster = [tail]
            last_timestamp = tail[1]
            distinct_receivers = 1

            #glogger.info("forming cluster from group:")
            #glogger.info("  0 = {r} {t:.1f}us".format(r=head[0].user, t=head[1]*1e6))

            for i in range(len(group) - 1, -1, -1):
                receiver, timestamp, error = group[i]
                #glogger.info("  consider {i} = {r} {t:.1f}us".format(i=i, r=receiver.user, t=timestamp*1e6))
                if (last_timestamp - timestamp) > 2e-3:
                    # Can't possibly be part of the same cluster.
                    #
                    # Note that this is a different test to the rough grouping above:
                    # that looks at the interval betwen _consecutive_ items, so a
                    # group might span a lot more than 2ms!
                    #glogger.info("   discard: >2ms out")
                    break

                # strict test for range, now.
                is_distinct = can_cluster = True
                for other_receiver, other_timestamp, other_error in cluster:
                    if other_receiver is receiver:
                        #glogger.info("   discard: duplicate receiver")
                        can_cluster = False
                        break

                    d = receiver.distance[other_receiver]
                    if abs(other_timestamp - timestamp) > (d * 1.05 + 1e3) / mlat.constants.Cair:
                        #glogger.info("   discard: delta {dt:.1f}us > max {m:.1f}us for range {d:.1f}m".format(
                        #    dt=abs(other_timestamp - timestamp)*1e6,
                        #    m=(d * 1.05 + 1e3) / mlat.constants.Cair*1e6,
                        #    d=d))
                        can_cluster = False
                        break

                    if d < 1e3:
                        # if receivers are closer than 1km, then
                        # only count them as one receiver for the 3-receiver
                        # requirement
                        #glogger.info("   not distinct vs receiver {r}".format(r=other_receiver.user))
                        is_distinct = False

                if can_cluster:
                    #glogger.info("   accept")
                    cluster.append(group[i])
                    del group[i]
                    if is_distinct:
                        distinct_receivers += 1

            if distinct_receivers >= 3:
                cluster.reverse()  # make it ascending timestamps again
                clusters.append((distinct_receivers, cluster))

    return clusters
