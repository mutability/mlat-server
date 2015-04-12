# -*- mode: python; indent-tabs-mode: nil -*-

import math
import time
import bisect


class Clock(object):
    """A particular clock. Stores characteristics of a clock,
    and acts as part of the key in the clock pairing map.

    For clocks with fixed synchronization, for example GPS time,
    all receivers will share the same Clock object.
    """

    def __init__(self, name, freq, max_drift_error, jitter):
        self.name = name
        self.freq = freq
        self.max_drift_error = max_drift_error
        self.jitter = jitter


GPS_NANO_CLOCK = Clock(name="GPS nanoseconds", freq=1e9, max_drift_error=1.0, jitter=50e-9)


def make_clock(receiver, clock_type, clock_freq):
    if clock_type == 'gps_nano':
        return GPS_NANO_CLOCK
    if clock_type == 'dump1090':
        return Clock(name='Clock for ' + receiver.user,
                     freq=12e6, max_drift_error=100.0, jitter=1e-6)
    raise NotImplementedError


class ClockPairing(object):
    """Describes the current relative characteristics of a pair of clocks."""

    K_DRIFT = 0.05

    def __init__(self, base_clock, peer_clock):
        self.base_clock = base_clock
        self.peer_clock = peer_clock
        self.drift = None
        self.n = 0
        self.ts_base = []
        self.ts_peer = []
        self.var = []
        self.var_sum = 0
        self.outliers = 0
        self.cumulative_error = 0

        self.relative_freq = peer_clock.freq / base_clock.freq
        self.drift_max = base_clock.max_drift_error + peer_clock.max_drift_error
        self.drift_max_delta = self.drift_max / 10.0
        self.outlier_threshold = 5 * (peer_clock.jitter + base_clock.jitter) * peer_clock.freq

        now = time.monotonic()
        self.expiry = now + 120.0
        self.validity = now + 30.0

    @property
    def variance(self):
        """Variance of recent predictions of the sync point versus the actual sync point."""
        if self.n == 0:
            return None
        return self.var_sum / self.n

    @property
    def error(self):
        """Standard error of recent predictions."""
        if self.n == 0:
            return None
        return math.sqrt(self.var_sum / self.n)

    @property
    def valid(self):
        """True if this pairing is usable for clock syncronization."""
        return self.n >= 2 and self.variance < 2500 and self.outliers == 0 and self.validity > time.monotonic()

    def update(self, base_ts, peer_ts, base_interval, peer_interval):
        """Update the relative drift and offset of this pairing given:

        base_ts: the timestamp of a recent point in time measured by the base clock
        peer_ts: the timestamp of the same point in time measured by the peer clock
        base_interval: the duration of a recent interval measured by the base clock
        peer_interval: the duration of the same interval measured by the peer clock
        """

        # clean old data
        self._prune_old_data(base_ts)

        # predict from existing data, compare to actual value
        if self.n > 0:
            prediction = self.predict_peer(base_ts)
            prediction_error = prediction - peer_ts

            if abs(prediction_error) > self.outlier_threshold and abs(prediction_error) > self.error * 5:
                self.outliers += 1
                if self.outliers < 5:
                    # don't accept this one
                    return
        else:
            prediction_error = 0  # first sync point, no error

        # update clock drift based on interval ratio
        # this might reject the update
        if not self._update_drift(base_interval, peer_interval):
            return

        # update clock offset based on the actual clock values
        self._update_offset(base_ts, peer_ts, prediction_error)
        self.outliers = max(0, self.outliers - 2)

        now = time.monotonic()
        self.expiry = now + 120.0
        self.validity = now + 30.0

    def _prune_old_data(self, latest_base_ts):
        i = 0
        while i < self.n and (latest_base_ts - self.ts_base[i]) > 30*self.base_clock.freq:
            i += 1

        if i > 0:
            del self.ts_base[0:i]
            del self.ts_peer[0:i]
            self.var_sum -= sum(self.var[0:i])
            del self.var[0:i]
            self.n -= i

    def _update_drift(self, base_interval, peer_interval):
        new_drift = (peer_interval / base_interval) / self.relative_freq - 1.0

        if abs(new_drift) > self.drift_max:
            # Bad data, ignore entirely
            return False

        if self.drift is None:
            # First sample, just trust it outright
            self.drift = new_drift
            return True

        drift_error = new_drift - self.drift
        if abs(drift_error) > self.drift_max_delta:
            # Too far away from the value we expect, discard
            return False

        # move towards the new value
        self.drift += drift_error * self.K_DRIFT
        return True

    def _update_offset(self, base_ts, peer_ts, prediction_error):
        # insert this into self.ts_base / self.ts_peer / self.var in the right place
        if self.n == 0:
            i = 0
        else:
            i = bisect.bisect_left(self.ts_base, base_ts)

            # ts_base and ts_peer define a function constructed by linearly
            # interpolating between each pair of values.
            #
            # This function must be monotonically increasing or one of our clocks
            # has effectively gone backwards. If this happens, give up and start
            # again.
            if (((i < self.n and self.ts_peer[i] < peer_ts) or
                 (i > 0 and self.ts_peer[i-1] > peer_ts))):
                self.ts_base = []
                self.ts_peer = []
                self.var = []
                self.var_sum = 0
                self.cumulative_error = 0
                self.n = 0
                i = 0

        self.n += 1
        self.ts_base.insert(i, base_ts)
        self.ts_peer.insert(i, peer_ts)

        p_var = prediction_error ** 2
        self.var.insert(i, p_var)
        self.var_sum += p_var
        self.cumulative_error += prediction_error

    def predict_peer(self, base_ts):
        if self.n == 0:
            return None

        i = bisect.bisect_left(self.ts_base, base_ts)
        if i == 0:
            # extrapolate before first point
            return (self.ts_peer[0] +
                    (base_ts - self.ts_base[0]) * self.relative_freq * (1 + self.drift))
        elif i == self.n:
            # extrapolate after last point
            return (self.ts_peer[-1] +
                    (base_ts - self.ts_base[-1]) * self.relative_freq * (1 + self.drift))
        else:
            # interpolate between two points
            return (self.ts_peer[i-1] +
                    (self.ts_peer[i] - self.ts_peer[i-1]) *
                    (base_ts - self.ts_base[i-1]) /
                    (self.ts_base[i] - self.ts_base[i-1]))

    def predict_base(self, peer_ts):
        if self.n == 0:
            return None

        i = bisect.bisect_left(self.ts_peer, peer_ts)
        if i == 0:
            # extrapolate before first point
            return (self.ts_base[0] +
                    (peer_ts - self.ts_peer[0]) / self.relative_freq / (1 + self.drift))
        elif i == self.n:
            # extrapolate after last point
            return (self.ts_base[-1] +
                    (peer_ts - self.ts_peer[-1]) / self.relative_freq / (1 + self.drift))
        else:
            # interpolate between two points
            return (self.ts_base[i-1] +
                    (self.ts_base[i] - self.ts_base[i-1]) *
                    (peer_ts - self.ts_peer[i-1]) /
                    (self.ts_peer[i] - self.ts_peer[i-1]))
