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
Clock normalization routines.
"""

import logging
import pygraph.classes.graph
import pygraph.algorithms.minmax

from mlat import profile

glogger = logging.getLogger("clocknorm")


_mst = profile.trackcpu(pygraph.algorithms.minmax.minimal_spanning_tree)

class _Predictor(object):
    """Simple object for holding prediction state"""
    def __init__(self, scale, offset, variance):
        self.scale = scale
        self.offset = offset
        self.variance = variance

    def predict(self, ts):
        return self.offset + ts * self.scale

    def combined_with(self, p):
        # return a predictor R such that:
        #   R.variance = self.variance + p.variance
        #   R.predict(x) = self.predict(p.predict(x))
        return _Predictor(self.scale * p.scale,
                          self.offset + p.offset * self.scale,
                          self.variance + p.variance)


def _make_predictors(clocktracker, station0, station1):
    """Return a tuple of predictors (p_01, p_10) where:

    p_01 will predict a station1 timestamp given a station0 timestamp
    p_10 will predict a station0 timestamp given a station1 timestamp

    Returns None if no suitable clock sync model is available for
    this pair of stations.
    """

    if station0 is station1:
        return None

    if station0.clock.epoch is not None and station0.clock.epoch == station1.clock.epoch:
        # Assume clocks are closely synchronized to the epoch (and therefore to each other)
        predictor = _Predictor(1.0, 0.0, station0.clock.jitter ** 2 + station1.clock.jitter ** 2)
        return (predictor, predictor)

    if station0 < station1:
        pairing = clocktracker.clock_pairs.get((station0, station1))
        if pairing is None or not pairing.valid:
            return None
        return (_Predictor(pairing.scale, pairing.offset, pairing.variance),
                _Predictor(pairing.i_scale, pairing.i_offset, pairing.variance))
    else:
        pairing = clocktracker.clock_pairs.get((station1, station0))
        if pairing is None or not pairing.valid:
            return None
        return (_Predictor(pairing.i_scale, pairing.i_offset, pairing.variance),
                _Predictor(pairing.scale, pairing.offset, pairing.variance))


def _label_heights(g, node, heights):
    """Label each node in the tree with a root of 'node'
    with its height, filling the map 'heights' which
    should be initially empty."""

    # we use heights as a visited-map too.
    heights[node] = 0
    for each in g.neighbors(node):
        if each not in heights:
            _label_heights(g, each, heights)
            mn = heights[each] + g.edge_weight((node, each))
            if mn > heights[node]:
                heights[node] = mn


def _tallest_branch(g, node, heights, ignore=None):
    """Find the edge in the tree rooted at 'node' that is part of
    the tallest branch. If ignore is not None, ignore that neighbour.
    Returns (pathlen,node)"""
    tallest = (0, None)

    for each in g.neighbors(node):
        if each is ignore:
            continue

        eh = heights[each] + g.edge_weight((node, each))
        if eh > tallest[0]:
            tallest = (eh, each)

    return tallest


def _convert_timestamps(g, timestamp_map, predictor_map, node, results, predictor):
    """Rewrite node and all unvisited nodes reachable from node using the
    chain of clocksync objects in conversion_chain, populating the results dict.

    node: the root node to convert
    timestamp_map: dict of node -> [(timestamp, utc), ...] to convert
    results: dict of node -> (variance, [(converted timestamp, utc), ...])
    predictor: current predictor to convert with
    """

    # convert our own timestamp using the provided chain
    r = []
    results[node] = (predictor.variance, r)   # also used as a visited-map
    for ts, utc in timestamp_map[node]:
        r.append((predictor.predict(ts), utc))

    # convert all reachable unvisited nodes
    for neighbor in g.neighbors(node):
        if neighbor not in results:
            sub_predictor = predictor_map[(neighbor, node)]
            # combined converts a timestamp using sub_predictor, then predictor
            combined = predictor.combined_with(sub_predictor)
            _convert_timestamps(g, timestamp_map, predictor_map,
                                neighbor,
                                results,
                                combined)


@profile.trackcpu
def normalize(clocktracker, timestamp_map):
    """
    Given {receiver: [(timestamp, utc), ...]}

    return [{receiver: (variance, [(timestamp, utc), ...])}, ...]
    where timestamps are normalized to some arbitrary base timescale within each map;
    one map is returned per connected subgraph."""

    # Represent the stations as a weighted graph where there
    # is an edge between S0 and S1 with weight W if we have a
    # sufficiently recent clock correlation between S0 and S1 with
    # estimated variance W.
    #
    # This graph may have multiple disconnected components. Treat
    # each separately and do this:
    #
    # Find the minimal spanning tree of the component. This will
    # give us the edges to use to convert between timestamps with
    # the lowest total error.
    #
    # Pick a central node of the MST to use as the the timestamp
    # basis, where a central node is a node that minimizes the maximum
    # path cost from the central node to any other node in the spanning
    # tree.
    #
    # Finally, convert all timestamps in the tree to the basis of the
    # central node.

    # populate initial graph
    g = pygraph.classes.graph.graph()
    g.add_nodes(timestamp_map.keys())

    # build a weighted graph where edges represent usable clock
    # synchronization paths, and the weight of each edge represents
    # the estimated variance introducted by converting a timestamp
    # across that clock synchronization.

    # also build a map of predictor objects corresponding to the
    # edges for later use

    predictor_map = {}
    for si in timestamp_map.keys():
        for sj in timestamp_map.keys():
            if si < sj:
                predictors = _make_predictors(clocktracker, si, sj)
                if predictors:
                    predictor_map[(si, sj)] = predictors[0]
                    predictor_map[(sj, si)] = predictors[1]
                    g.add_edge((si, sj), wt=predictors[0].variance)

    # find a minimal spanning tree for each component of the graph
    mst_forest = _mst(g)

    # rebuild the graph with only the spanning edges, retaining weights
    # also note the roots of each tree as we go
    g = pygraph.classes.graph.graph()
    g.add_nodes(mst_forest.keys())
    roots = []
    for edge in mst_forest.items():
        if edge[1] is None:
            roots.append(edge[0])
        else:
            g.add_edge(edge, wt=predictor_map[edge].variance)

    # for each spanning tree, find a central node and convert timestamps
    components = []
    for root in roots:
        # label heights of nodes, where the height of a node is
        # the length of the most expensive path to a child of the node
        heights = {}
        _label_heights(g, root, heights)

        # Find the longest path in the spanning tree; we want to
        # resolve starting at the center of this path, as this minimizes
        # the maximum path length to any node

        # find the two tallest branches leading from the root
        tall1 = _tallest_branch(g, root, heights)
        tall2 = _tallest_branch(g, root, heights, ignore=tall1[1])

        # Longest path is TALL1 - ROOT - TALL2
        # We want to move along the path into TALL1 until the distances to the two
        # tips of the path are equal length. This is the same as finding a node on
        # the path within TALL1 with a height of about half the longest path.
        target = (tall1[0] + tall2[0]) / 2
        central = root
        step = tall1[1]
        while step and abs(heights[central] - target) > abs(heights[step] - target):
            central = step
            _, step = _tallest_branch(g, central, heights, ignore=central)

        # Convert timestamps so they are using the clock of "central"
        # by walking the spanning tree edges.
        results = {}
        predictor = _Predictor(1.0, 0.0, central.clock.jitter**2)
        _convert_timestamps(g, timestamp_map, predictor_map, central, results, predictor)
        components.append(results)

    return components


@profile.trackcpu
def build_normalization_map(clocktracker):
    """Returns a dict:

    {receiver: (id, predictor), ...}

    where all receivers that can be mutually synchronized have the same ID,
    and the predictors convert to a common clock for each ID."""

    pairings = clocktracker.clock_pairs

    g = pygraph.classes.graph.graph()

    # build a weighted graph where edges represent usable clock
    # synchronization paths, and the weight of each edge represents
    # the estimated variance introducted by converting a timestamp
    # across that clock synchronization.

    # also build a map of predictor objects corresponding to the
    # edges for later use

    receivers = set()
    receivers_by_epoch = {}
    predictor_map = {}
    for si, sj in pairings.keys():
        predictors = _make_predictors(clocktracker, si, sj)
        if predictors:
            predictor_map[(si, sj)] = predictors[0]
            predictor_map[(sj, si)] = predictors[1]

            if si not in receivers:
                receivers.add(si)
                if si.clock.epoch is not None:
                    receivers_by_epoch.setdefault(si.clock.epoch, []).append(si)
                g.add_node(si)

            if sj not in receivers:
                receivers.add(sj)
                if sj.clock.epoch is not None:
                    receivers_by_epoch.setdefault(sj.clock.epoch, []).append(sj)
                g.add_node(sj)

            g.add_edge((si, sj), wt=predictors[0].variance)

    # also add pairings for receivers with the same epoch,
    # regardless of whether they actually have ADS-B sync or not
    for receiver_list in receivers_by_epoch.values():
        for si in receiver_list:
            for sj in receiver_list:
                if si < sj and (si, sj) not in predictor_map:
                    predictors = _make_predictors(clocktracker, si, sj)
                    if predictors:
                        predictor_map[(si, sj)] = predictors[0]
                        predictor_map[(sj, si)] = predictors[1]
                        g.add_edge((si, sj), wt=predictors[0].variance)

    # find a minimal spanning tree for each component of the graph
    mst_forest = _mst(g)

    # rebuild the graph with only the spanning edges, retaining weights
    # also note the roots of each tree as we go
    g = pygraph.classes.graph.graph()
    g.add_nodes(mst_forest.keys())
    roots = []
    for edge in mst_forest.items():
        if edge[1] is None:
            roots.append(edge[0])
        else:
            g.add_edge(edge, wt=predictor_map[edge].variance)

    # for each spanning tree, find a central node and build the final predictors
    results = {}
    maxdepth = 0

    for component_id in range(len(roots)):
        root = roots[component_id]

        # label heights of nodes, where the height of a node is
        # the length of the most expensive path to a child of the node
        heights = {}
        _label_heights(g, root, heights)

        # Find the longest path in the spanning tree; we want to
        # resolve starting at the center of this path, as this minimizes
        # the maximum path length to any node

        # find the two tallest branches leading from the root
        tall1 = _tallest_branch(g, root, heights)
        tall2 = _tallest_branch(g, root, heights, ignore=tall1[1])

        # Longest path is TALL1 - ROOT - TALL2
        # We want to move along the path into TALL1 until the distances to the two
        # tips of the path are equal length. This is the same as finding a node on
        # the path within TALL1 with a height of about half the longest path.
        target = (tall1[0] + tall2[0]) / 2
        central = root
        step = tall1[1]
        while step and abs(heights[central] - target) > abs(heights[step] - target):
            central = step
            _, step = _tallest_branch(g, central, heights, ignore=central)

        # build predictors that convert to "central"
        # the graph only has spanning tree edges so it doesn't matter what
        # order we walk it in, there's only one path from "central" to
        # each node.

        results[central] = (component_id, _Predictor(1.0, 0.0, central.clock.jitter**2))

        queue = [(central, 0)]
        while queue:
            node, depth = queue.pop()
            maxdepth = max(depth, maxdepth)
            _, predictor = results[node]

            for neighbor in g.neighbors(node):
                if neighbor not in results:
                    next_predictor = predictor_map[(neighbor, node)]
                    combined = predictor.combined_with(next_predictor)
                    results[neighbor] = (component_id, combined)
                    queue.append((neighbor, depth + 1))

    #glogger.info("max norm map depth was {d}".format(d=maxdepth))
    return results


@profile.trackcpu
def normalize_via_map(normalization_map, timestamp_map):
    """Like normalize, but takes a normalization map made by
    build_normalization_map to speed things up."""

    results = {}
    for receiver, timestamp_list in timestamp_map.items():
        norm = normalization_map.get(receiver)
        if norm is None:
            # ditch it, not syncable
            continue

        component_id, predictor = norm
        rdict = results.setdefault(component_id, {})
        converted = [(predictor.predict(ts), utc) for ts, utc in timestamp_list]
        rdict[receiver] = (predictor.variance, converted)

    return list(results.values())
