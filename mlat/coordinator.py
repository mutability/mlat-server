# -*- mode: python; indent-tabs-mode: nil -*-

import asyncio
import json
from contextlib import closing

from mlat import tracker
from mlat import clocksync
from mlat import clocktrack


class ReceiverHandle(object):
    """Represents a particular connected receiver and the associated
    connection that manages it."""

    def __init__(self, user, connection, clock, position):
        self.user = user
        self.connection = connection
        self.clock = clock
        self.position = position
        self.dead = False

        self.sync_count = 0
        self.last_rate_report = None
        self.tracking = set()
        self.sync_interest = set()
        self.mlat_interest = set()
        self.requested = set()

    def update_interest_sets(self, new_sync, new_mlat):
        for added in new_sync.difference(self.sync_interest):
            added.sync_interest.add(self)

        for removed in self.sync_interest.difference(new_sync):
            removed.sync_interest.remove(self)

        for added in new_mlat.difference(self.mlat_interest):
            added.mlat_interest.add(self)

        for removed in self.mlat_interest.difference(new_mlat):
            removed.mlat_interest.remove(self)

        self.sync_interest = new_sync
        self.mlat_interest = new_mlat

    def refresh_traffic_requests(self):
        self.requested = {x for x in self.tracking if x.interesting}
        self.connection.request_traffic(self, {x.icao for x in self.requested})

    def __lt__(self, other):
        return id(self) < id(other)

    def __str__(self):
        return self.user

    def __repr__(self):
        return 'ReceiverHandle({0!r},{1!r})@{2}'.format(self.user,
                                                        self.connection,
                                                        id(self))


class Coordinator(object):
    """Master coordinator. Receives all messages from receivers and dispatches
    them to clock sync / multilateration / tracking as needed."""

    def __init__(self, authenticator=None):
        """Coordinator(authenticator=None) -> coordinator object.

If authenticator is not None, it should be a callable that takes two arguments:
the newly created ReceiverHandle, plus the 'auth' argument provided by the connection.
The authenticator may modify the handle if needed. The authenticator should either
return silently on success, or raise an exception (propagated to the caller) on
failure.
"""

        self.receivers = {}    # keyed by username
        self.authenticator = authenticator
        self.tracker = tracker.Tracker()
        self.clock_tracker = clocktrack.ClockTracker()
        asyncio.get_event_loop().call_later(30.0, self._write_state)

    def _write_state(self):
        asyncio.get_event_loop().call_later(30.0, self._write_state)

        state = {'receivers': {},
                 'aircraft': {}}

        for r in self.receivers.values():
            state['receivers'][r.user] = {
                'traffic': ['{0:06X}'.format(x) for x in r.requested],
                'tracking': ['{0:06X}'.format(x.icao) for x in r.tracking],
                'sync_interest': ['{0:06X}'.format(x.icao) for x in r.sync_interest],
                'mlat_interest': ['{0:06X}'.format(x.icao) for x in r.mlat_interest],
                'clocksync': self.clock_tracker.dump_receiver_state(r)
            }

        with closing(open('state.json', 'w')) as f:
            json.dump(state, fp=f)

    def new_receiver(self, connection, user, auth, position, clock_type):
        """Assigns a new receiver ID for a given user.
        Returns the new receiver ID.

        May raise ValueError to disallow this receiver."""

        if user in self.receivers:
            raise ValueError('User {user} is already connected'.format(user=user))

        clock = clocksync.make_clock(clock_type)
        handle = ReceiverHandle(user, connection, clock, position)

        if self.authenticator is not None:
            self.authenticator(handle, auth)  # may raise ValueError if authentication fails

        self.receivers[handle.user] = handle  # authenticator might update user
        return handle

    def receiver_disconnect(self, receiver):
        """Notes that the given receiver has disconnected."""

        receiver.dead = True
        if self.receivers.get(receiver.user) is receiver:
            self.tracker.remove_all(receiver)
            self.clock_tracker.receiver_disconnect(receiver)
            del self.receivers[receiver.user]

    def receiver_sync(self, receiver,
                      even_time, odd_time, even_message, odd_message):
        """Receive a DF17 message pair for clock synchronization."""
        self.clock_tracker.receiver_sync(receiver,
                                         even_time, odd_time,
                                         even_message, odd_message)

    def receiver_mlat(self, receiver, timestamp, message):
        """Receive a message for multilateration."""
        pass

    def receiver_tracking_add(self, receiver, icao_set):
        """Update a receiver's tracking set by adding some aircraft."""
        self.tracker.add(receiver, icao_set)
        if receiver.last_rate_report is None:
            # not receiving rate reports for this receiver
            self.tracker.update_interest(receiver)

    def receiver_tracking_remove(self, receiver, icao_set):
        """Update a receiver's tracking set by removing some aircraft."""
        self.tracker.remove(receiver, icao_set)
        if receiver.last_rate_report is None:
            # not receiving rate reports for this receiver
            self.tracker.update_interest(receiver)

    def receiver_clock_reset(self, receiver):
        """Reset current clock synchronization for a receiver."""
        pass

    def receiver_rate_report(self, receiver, report):
        """Process an ADS-B position rate report for a receiver."""
        receiver.last_rate_report = report
        self.tracker.update_interest(receiver)
