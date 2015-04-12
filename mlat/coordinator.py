# -*- mode: python; indent-tabs-mode: nil -*-

from .tracker import Tracker


class ReceiverHandle(object):
    """Represents a particular connected receiver and the associated
    connection that manages it."""

    def __init__(self, user, connection, clock_epoch, clock_freq):
        self.user = user
        self.connection = connection
        self.clock_epoch = clock_epoch
        self.clock_freq = clock_freq

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
        self.tracker = Tracker(self)

    def new_receiver(self, connection, user, auth, clock_epoch, clock_freq):
        """Assigns a new receiver ID for a given user.
        Returns the new receiver ID.

        Keyword args provide user authentication.

        May raise ValueError to disallow this receiver."""

        if user in self.receivers:
            raise ValueError('User {user} is already connected'.format(user=user))

        handle = ReceiverHandle(user, connection, clock_epoch, clock_freq)

        if self.authenticator is not None:
            self.authenticator(handle, auth)  # may raise ValueError if authentication fails

        self.receivers[handle.user] = handle  # authenticator might update user
        return handle

    def receiver_disconnect(self, receiver):
        """Notes that the given receiver has disconnected."""

        if self.receivers.get(receiver.user) is receiver:
            # TODO: clock cleanup, once we're doing clock sync
            self.tracker.remove_all(receiver)
            del self.receivers[receiver.user]

    def receiver_sync(self, receiver,
                      even_time, odd_time, even_message, odd_message):
        """Receive a DF17 message pair for clock synchronization."""
        pass

    def receiver_mlat(self, receiver, timestamp, message):
        """Receive a message for multilateration."""
        pass

    def receiver_tracking_add(self, receiver, icao_set):
        """Update a receiver's tracking set by adding some aircraft."""
        self.tracker.add(receiver, icao_set)

    def receiver_tracking_remove(self, receiver, icao_set):
        """Update a receiver's tracking set by removing some aircraft."""
        self.tracker.remove(receiver, icao_set)

    def receiver_clock_reset(self, receiver):
        """Reset current clock synchronization for a receiver."""
        pass
