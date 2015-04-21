# -*- mode: python; indent-tabs-mode: nil -*-

import logging

import scipy.optimize

import mlat.config
from mlat import geodesy
from mlat.constants import Cair

# The core of it all. Not very big, is it?
# (Admittedly the entire least-squares solver is hidden within scipy..)

glogger = logging.getLogger("solver")


def _residuals(x_guess, pseudorange_data, altitude):
    """Return an array of residuals for a position guess at x_guess versus
    actual measurements pseudorange_data and altitude."""

    (*position_guess, offset) = x_guess

    res = []

    # compute pseudoranges at the current guess vs. measured pseudorange
    for receiver_position, pseudorange, error in pseudorange_data:
        pseudorange_guess = geodesy.ecef_distance(receiver_position, position_guess) - offset
        res.append((pseudorange - pseudorange_guess) / error)

    # compute altitude at the current guess vs. measured altitude
    _, _, altitude_guess = geodesy.ecef2llh(position_guess)
    res.append((altitude - altitude_guess) / 150)  # hardcoded error estimate, ~500ft

    return res


def solve(measurements, altitude, initial_guess):
    """Given a set of receive timestamps, multilaterate the position of the transmitter.

    measurements: a list of (receiver, timestamp, error) tuples. Should be sorted by timestamp.
      receiver.position should be the ECEF position of the receiver
      timestamp should be a reception time in seconds (with an arbitrary epoch)
      error should be the estimated error in timestamp
    altitude: the reported altitude of the transmitter in _meters_
    initial_guess: an ECEF position to start the solver from

    Returns None on failure, or (ecef, ecef_cov) on success, with:

    ecef: the multilaterated ECEF position of the transmitter
    ecef_cov: an estimate of the covariance matrix of ecef
    """

    base_timestamp = measurements[0][1]
    pseudorange_data = [(receiver.position, (timestamp - base_timestamp) * Cair, error * Cair)
                        for receiver, timestamp, error in measurements]
    x_guess = [initial_guess[0], initial_guess[1], initial_guess[2], 0.0]
    x_est, cov_x, infodict, mesg, ler = scipy.optimize.leastsq(
        _residuals,
        x_guess,
        args=(pseudorange_data, altitude),
        full_output=True,
        maxfev=mlat.config.SOLVER_MAXFEV)

    if ler in (1, 2, 3, 4):
        #glogger.info("solver success: {0} {1}".format(ler, mesg))

        # Solver found a result. Validate that it makes
        # some sort of physical sense.
        (*position_est, offset_est) = x_est

        if offset_est < 0 or offset_est > mlat.config.MAX_RANGE:
            #glogger.info("solver: bad offset: {0}".formaT(offset_est))
            # implausible range offset to closest receiver
            return None

        for receiver, timestamp, error in measurements:
            d = geodesy.ecef_distance(receiver.position, position_est)
            if d > mlat.config.MAX_RANGE:
                # too far from this receiver
                #glogger.info("solver: bad range: {0}".format(d))
                return None

        if cov_x is None:
            return position_est, None
        else:
            return position_est, cov_x[0:3, 0:3]

    else:
        # Solver failed
        #glogger.info("solver: failed: {0} {1}".format(ler, mesg))
        return None
