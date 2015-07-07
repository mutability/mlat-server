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
Maintains clock synchronization between individual pairs of receivers.
"""

import math
import logging
from contextlib import closing

__all__ = ('Clock', 'ClockPairing', 'make_clock')

glogger = logging.getLogger("clocksync")


class Clock(object):
    """A particular clock. Stores characteristics of a clock,
    and acts as part of the key in the clock pairing map.
    """

    def __init__(self, epoch, freq, max_freq_error, jitter):
        """Create a new clock representation.

        epoch: a string indicating a fixed epoch, or None if freerunning
        freq: the clock frequency in Hz (float)
        max_freq_error: the maximum expected relative frequency error (i.e. 1e-6 is 1PPM) (float)
        jitter: the expected jitter of a typical reading, in seconds, standard deviation  (float)
        """
        self.epoch = epoch
        self.freq = freq
        self.max_freq_error = max_freq_error
        self.jitter = jitter


def make_clock(clock_type):
    """Return a new Clock instance for the given clock type."""

    if clock_type == 'radarcape_gps':
        return Clock(epoch='gps_midnight', freq=1e9, max_freq_error=1e-6, jitter=15e-9)
    if clock_type == 'beast':
        return Clock(epoch=None, freq=12e6, max_freq_error=5e-6, jitter=83e-9)
    if clock_type == 'sbs':
        return Clock(epoch=None, freq=20e6, max_freq_error=100e-6, jitter=500e-9)
    if clock_type == 'dump1090':
        return Clock(epoch=None, freq=12e6, max_freq_error=100e-6, jitter=500e-9)
    raise NotImplementedError


class ClockPairing(object):
    """Describes the current relative characteristics of a pair of clocks."""

    KP = 0.05
    KI = 0.01

    def __init__(self, base, peer):
        self.base = base
        self.peer = peer
        self.base_clock = base.clock
        self.peer_clock = peer.clock

        self.drift_max = base.clock.max_freq_error + peer.clock.max_freq_error
        self.drift_max_delta = self.drift_max / 10.0
        self.outlier_threshold = 5 * math.sqrt(peer.clock.jitter ** 2 + base.clock.jitter ** 2)   # 5 sigma

        # PI controller for relative clock drift term
        self.raw_drift = None
        self.cumulative_error = 0.0

        # and the output of the drift controller
        self.drift = None
        self.i_drift = None

        self.reset()

    def reset(self):
        self.base_ref = None
        self.peer_ref = None
        self.recent_sync_count = self.prev_sync_count = 0
        self.recent_var_sum = self.prev_var_sum = 0.0
        self.outliers = 0
        self._update_derived()

    def periodic_update(self):
        self.prev_sync_count = self.recent_sync_count
        self.recent_sync_count = 0
        self.prev_var_sum = self.recent_var_sum
        self.recent_var_sum = 0.0
        self._update_derived()

    def _update_derived(self):
        self.sync_count = self.recent_sync_count + self.prev_sync_count
        if not self.sync_count:
            self.variance = None
            self.error = None
        else:
            self.variance = (self.recent_var_sum + self.prev_var_sum) / self.sync_count
            self.error = math.sqrt(self.variance)

        if self.raw_drift is None:
            self.drift = self.i_drift = None
        else:
            self.drift = self.raw_drift - self.KI * self.cumulative_error
            self.i_drift = -self.drift / (1.0 + self.drift)

    @property
    def valid(self):
        """True if this pairing is usable for clock syncronization."""
        return bool(self.sync_count >= 2 and
                    self.error < 4e-6 and
                    self.outliers == 0)

    def update(self, address, base_ts, peer_ts, base_interval, peer_interval, base_distance, base_bearing, peer_distance, peer_bearing):
        """Update the relative drift and offset of this pairing given:

        base_ts: the timestamp of a recent point in time measured by the base clock
        peer_ts: the timestamp of the same point in time measured by the peer clock
        base_interval: the duration of a recent interval measured by the base clock
        peer_interval: the duration of the same interval measured by the peer clock

        Returns True if the update was used, False if it was discarded.
        """

        # check drift, discard out of range values early
        new_drift = (peer_interval - base_interval) / base_interval
        if abs(new_drift) > self.drift_max:
            # Bad data, ignore entirely
            return False

        outlier = False

        if self.raw_drift is not None:
            drift_error = new_drift - self.raw_drift
            if abs(drift_error) > self.drift_max_delta:
                # Too far away from the value we expect
                outlier = True

        # predict from existing data, compare to actual value
        if self.sync_count > 0:
            if base_ts < self.base_ref and peer_ts < self.peer_ref:
                # it's in the past, discard
                return False

            prediction = self.predict_peer(base_ts)
            prediction_error = prediction - peer_ts

            if ((base_ts < self.base_ref or peer_ts < self.peer_ref or
                 (abs(prediction_error) > self.outlier_threshold and abs(prediction_error) > self.error * 5))):
                outlier = True
        else:
            prediction = None
            prediction_error = 0  # first sync point, no error

        if outlier:
            self.outliers += 1
            if self.outliers < 5:
                # don't accept this one
                return False

        if abs(prediction_error) > self.outlier_threshold:
            if prediction_error > 0:
                glogger.info("{r}: {peer} clock was {e:.1f}us slower than predicted, {a:06X} {d0:.1f}@{b0:.0f} / {d1:.1f}@{b1:.0f}".format(
                    r=self, peer=self.peer, e=prediction_error * 1e6, a=address, d0=base_distance/1e3, b0=base_bearing, d1=peer_distance/1e3, b1=peer_bearing))
            else:
                glogger.info("{r}: {peer} clock was {e:.1f}us faster than predicted, {a:06X} {d0:.1f}@{b0:.0f} / {d1:.1f}@{b1:.0f}".format(
                    r=self, peer=self.peer, e=prediction_error * -1e6, a=address, d0=base_distance/1e3, b0=base_bearing, d1=peer_distance/1e3, b1=peer_bearing))

            with closing(open('steps.csv', 'a')) as f:
                print('{base},{peer},{address:06X},{base_ts:.7f},{peer_ts:.7f},{error:.1f},{base_distance:.0f},{base_bearing:.0f},{peer_distance:.0f},{peer_bearing:.0f}'.format(
                    base=self.base,
                    peer=self.peer,
                    address=address,
                    base_ts=base_ts,
                    peer_ts=peer_ts,
                    error=prediction_error*1e6,
                    base_distance=base_distance,
                    base_bearing=base_bearing,
                    peer_distance=peer_distance,
                    peer_bearing=peer_bearing), file=f)

        # update drift
        if self.raw_drift is None:
            # First sample, just trust it outright
            self.raw_drift = new_drift
        else:
            # move towards the new value
            self.raw_drift += drift_error * self.KP

        # update clock offset based on the actual clock values

        if prediction is None or abs(prediction_error) > 10e-6:
            # converge directly to the new value
            self.base_ref = base_ts
            self.peer_ref = peer_ts
        else:
            # smooth this a little as there's inherent jitter in the beacon measurements due
            #  * underlying errors in the aircraft's measured position
            #  * aircraft motion between determining the position and transmitting it
            self.base_ref = base_ts
            self.peer_ref = prediction - prediction_error * 0.5

        p_var = prediction_error ** 2
        self.recent_var_sum += p_var
        self.recent_sync_count += 1

        # if we are accepting an outlier, do not include it in our integral term
        if not self.outliers:
            self.cumulative_error = max(-50e-6, min(50e-6, self.cumulative_error + prediction_error))  # limit to 50us

        self.outliers = 0
        self._update_derived()
        return True

    def predict_peer(self, base_ts):
        """
        Given a time from the base clock, predict the time of the peer clock.
        """

        if not self.sync_count:
            return None

        # extrapolate after anchor point
        elapsed = base_ts - self.base_ref
        return (self.peer_ref +
                elapsed +
                elapsed * self.drift)

    def predict_base(self, peer_ts):
        """
        Given a time from the peer clock, predict the time of the base
        clock.
        """

        if not self.sync_count:
            return None

        # extrapolate after anchor point
        elapsed = peer_ts - self.peer_ref
        return (self.base_ref +
                elapsed +
                elapsed * self.i_drift)

    def __str__(self):
        return self.base.uuid + ':' + self.peer.uuid
