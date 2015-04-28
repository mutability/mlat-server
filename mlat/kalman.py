# -*- mode: python; indent-tabs-mode: nil -*-

import math
import numpy
import pykalman.unscented
import functools
import logging

import mlat.geodesy
import mlat.constants

glogger = logging.getLogger("kalman")


def _kalman_observation(state, *, positions):
    """Kalman filter observation function.

    Given state (position,velocity) and a list of N receiver positions,
    return N-1 pseudorange observations (each relative to the first receiver's
    pseudorange) and an altitude."""

    x, y, z, vx, vy, vz = state

    n = len(positions)
    obs = numpy.zeros(n)

    _, _, alt = mlat.geodesy.ecef2llh((x, y, z))
    obs[0] = alt

    zx, zy, zz = positions[0]
    zero_range = ((zx - x)**2 + (zy - y)**2 + (zz - z)**2)**0.5

    for i in range(1, n):
        rx, ry, rz = positions[i]
        obs[i] = ((rx - x)**2 + (ry - y)**2 + (rz - z)**2)**0.5 - zero_range

    return obs


def _kalman_transition(state, *, dt):
    """Kalman filter transition function.

    Given state (position,velocity) and a time delta dt,
    return the updated state after dt.

    This is a constant-velocity model."""

    x, y, z, vx, vy, vz = state
    return numpy.array([x + vx*dt, y + vy*dt, z + vz*dt, vx, vy, vz])


class KalmanState(object):
    """Kalman filter state for a single aircraft"""

    def __init__(self, ac):
        self.ac = ac
        self._reset()

    def _reset(self):
        # the filter itself:
        self._mean = None
        self._cov = None
        self._new = True
        self._outliers = 0
        self.last_update = None

        # does the filter have useful derived data?
        self.valid = False

        # most recent values derived from filter state
        self.position = None        # ECEF
        self.velocity = None        # ECEF
        self.position_error = None  # meters
        self.velocity_error = None  # m/s

        # .. some derived values in more useful reference frames
        self.position_llh = None    # LLH
        self.velocity_enu = None    # ENU
        self.heading = None         # degrees
        self.ground_speed = None    # m/s
        self.vertical_speed = None  # m/s

    def _update_derived(self):
        """Update derived values from self._mean and self._cov"""

        if self._mean is None or self._new:
            self.valid = False
            return

        self.position = self._mean[0:3]
        self.velocity = self._mean[3:6]

        pe = numpy.trace(self._cov[0:3, 0:3])
        self.position_error = 0 if pe < 0 else math.sqrt(pe)
        ve = numpy.trace(self._cov[3:6, 3:6])
        self.velocity_error = 0 if ve < 0 else math.sqrt(ve)

        lat, lon, alt = self.position_llh = mlat.geodesy.ecef2llh(self.position)

        # rotate velocity into the local tangent plane
        lat_r = lat * mlat.constants.DTOR
        lon_r = lon * mlat.constants.DTOR
        C = numpy.array([[-math.sin(lon_r), math.cos(lon_r), 0],
                         [math.sin(-lat_r) * math.cos(lon_r), math.sin(-lat_r) * math.sin(lon_r), math.cos(-lat_r)],
                         [math.cos(-lat_r) * math.cos(lon_r), math.cos(-lat_r) * math.sin(lon_r), -math.sin(-lat_r)]])
        east, north, up = numpy.dot(C, self.velocity.T).T

        # extract speeds, headings
        self.heading = math.atan2(east, north) * 180.0 / math.pi
        if self.heading < 0:
            self.heading += 360
        self.ground_speed = math.sqrt(north**2 + east**2)
        self.vertical_speed = up

        self.valid = True

    def update(self, position_time, measurements, altitude, leastsquares_position, leastsquares_cov, distinct):
        """Update the filter given a new set of observations.

        position_time:         the time of these measurements, UTC seconds
        measurements:          a list of (receiver, timestamp, variance) tuples
        altitude:              reported altitude in meters
        leastsquares_position: the ECEF position computed by the least-squares solver
        distinct:              the number of distinct receivers (<= len(measurements))
        """

        if self._mean is None or (position_time - self.last_update > 60.0):
            # reinitialize
            self._reset()
            if distinct >= 4:
                # accept this
                self.last_update = position_time
                self._mean = numpy.array(list(leastsquares_position) + [0, 0, 0])
                self._cov = numpy.zeros((6, 6))
                self._cov[0:3, 0:3] = leastsquares_cov
                self._cov[3, 3] = self._cov[4, 4] = self._cov[5, 5] = 200**2
                return
            else:
                # nope.
                return

        if self._new and distinct < 4:
            # don't trust 3 station results until we have converged
            return

        # update filter
        zero_pr = measurements[0][1] * mlat.constants.Cair
        positions = [measurements[0][0].position]

        n = len(measurements)
        obs = numpy.zeros(n)
        obs_var = numpy.zeros(n)

        obs[0] = altitude
        obs_var[0] = 50**2

        for i in range(1, n):
            receiver, timestamp, variance = measurements[i]
            positions.append(receiver.position)
            obs[i] = timestamp * mlat.constants.Cair - zero_pr
            obs_var[i] = (variance + measurements[0][2]) * mlat.constants.Cair**2

        obs_covar = numpy.diag(obs_var)

        dt = position_time - self.last_update
        accel = 0.5   # m/s^2

        trans_covar = numpy.zeros((6, 6))
        trans_covar[0, 0] = trans_covar[1, 1] = trans_covar[2, 2] = 0.25*dt**4
        trans_covar[0, 3] = trans_covar[3, 0] = 0.5*dt**3
        trans_covar[1, 4] = trans_covar[4, 1] = 0.5*dt**3
        trans_covar[2, 5] = trans_covar[5, 2] = 0.5*dt**3
        trans_covar[3, 3] = trans_covar[4, 4] = trans_covar[5, 5] = dt**2

        trans_covar *= accel**2

        try:
            transition_function = functools.partial(_kalman_transition, dt=dt)
            observation_function = functools.partial(_kalman_observation, positions=positions)

            #
            # This is extracted from pykalman's AdditiveUnscentedFilter.filter_update()
            # because we want to access the intermediate (prediction) result to decide
            # whether to accept this observation or not.
            #

            # make sigma points
            moments_state = pykalman.unscented.Moments(self._mean, self._cov)
            points_state = pykalman.unscented.moments2points(moments_state)

            # Predict.
            (_, moments_pred) = (
                pykalman.unscented.unscented_filter_predict(
                    transition_function=transition_function,
                    points_state=points_state,
                    sigma_transition=trans_covar
                )
            )
            points_pred = pykalman.unscented.moments2points(moments_pred)

            # Decide whether this is an outlier:
            # Get the current filter state mean and covariance
            # as an observation:
            (obs_points_pred, obs_moments_pred) = (
                pykalman.unscented.unscented_transform(
                    points_pred, observation_function,
                    sigma_noise=obs_covar
                )
            )

            # Find the Mahalanobis distance between the filter observation
            # and our new observation, using the filter's observation
            # covariance as our expected distribution.
            innovation = obs - obs_moments_pred.mean
            vi = numpy.linalg.inv(obs_moments_pred.covariance)
            mdsq = numpy.dot(numpy.dot(innovation.T, vi), innovation)

            # If the Mahalanobis distance is very large this observation is an outlier
            if mdsq > 100:
                glogger.info("{ac.icao:06X} skip innov={innovation} mdsq={mdsq}".format(
                    ac=self.ac,
                    innovation=innovation,
                    mdsq=mdsq))

                self._outliers += 1
                if self._outliers < 3:
                    # don't use this one
                    return
                glogger.info("{ac.icao:06X} reset due to outliers.".format(ac=self.ac))
                self._reset()
                return

            self._outliers = 0

            # correct filter state using the current observations
            (self._mean, self._cov) = (
                pykalman.unscented.unscented_filter_correct(
                    observation_function=observation_function,
                    moments_pred=moments_pred,
                    points_pred=points_pred,
                    observation=obs,
                    sigma_observation=obs_covar
                )
            )

            # converged enough to start reporting?
            if self._new and numpy.trace(self._cov) < 2e6:
                glogger.info("{ac.icao:06X} acquired.".format(ac=self.ac))
                self._new = False

            self.last_update = position_time
            self._update_derived()

        except Exception:
            glogger.exception("Kalman filter update failed. " +
                              "dt={dt} obs={obs} obs_covar={obs_covar} mean={mean} covar={covar}".format(
                                  dt=dt,
                                  obs=obs,
                                  obs_covar=obs_covar,
                                  mean=self._mean,
                                  covar=self._cov))
            self._reset()
            return
