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
Decoder for the Compact Position Reporting (CPR) position encoding used in
ADS-B extended squitter messages.
"""

import math
import bisect

__all__ = ['decode']


nl_table = (
    (10.47047130, 59),
    (14.82817437, 58),
    (18.18626357, 57),
    (21.02939493, 56),
    (23.54504487, 55),
    (25.82924707, 54),
    (27.93898710, 53),
    (29.91135686, 52),
    (31.77209708, 51),
    (33.53993436, 50),
    (35.22899598, 49),
    (36.85025108, 48),
    (38.41241892, 47),
    (39.92256684, 46),
    (41.38651832, 45),
    (42.80914012, 44),
    (44.19454951, 43),
    (45.54626723, 42),
    (46.86733252, 41),
    (48.16039128, 40),
    (49.42776439, 39),
    (50.67150166, 38),
    (51.89342469, 37),
    (53.09516153, 36),
    (54.27817472, 35),
    (55.44378444, 34),
    (56.59318756, 33),
    (57.72747354, 32),
    (58.84763776, 31),
    (59.95459277, 30),
    (61.04917774, 29),
    (62.13216659, 28),
    (63.20427479, 27),
    (64.26616523, 26),
    (65.31845310, 25),
    (66.36171008, 24),
    (67.39646774, 23),
    (68.42322022, 22),
    (69.44242631, 21),
    (70.45451075, 20),
    (71.45986473, 19),
    (72.45884545, 18),
    (73.45177442, 17),
    (74.43893416, 16),
    (75.42056257, 15),
    (76.39684391, 14),
    (77.36789461, 13),
    (78.33374083, 12),
    (79.29428225, 11),
    (80.24923213, 10),
    (81.19801349, 9),
    (82.13956981, 8),
    (83.07199445, 7),
    (83.99173563, 6),
    (84.89166191, 5),
    (85.75541621, 4),
    (86.53536998, 3),
    (87.00000000, 2),
    (90.00000000, 1)
)

nl_lats = [x[0] for x in nl_table]
nl_vals = [x[1] for x in nl_table]


def NL(lat):
    if lat < 0:
        lat = -lat

    nl = nl_vals[bisect.bisect_left(nl_lats, lat)]
    return nl


def MOD(a, b):
    r = a % b
    if r < 0:
        r += b
    return r


def decode(latE, lonE, latO, lonO):
    """Perform globally unambiguous position decoding for a pair of
    airborne CPR messages.

    latE, lonE: the raw latitude and longitude values of the even message
    latO, lonO: the raw latitude and longitude values of the odd message

    Return a tuple of (even latitude, even longitude, odd latitude, odd longitude)

    Raises ValueError if the messages do not produce a useful position."""

    # Compute the Latitude Index "j"
    j = math.floor(((59 * latE - 60 * latO) / 131072.0) + 0.5)
    rlatE = (360.0 / 60.0) * (MOD(j, 60) + latE / 131072.0)
    rlatO = (360.0 / 59.0) * (MOD(j, 59) + latO / 131072.0)

    # adjust for southern hemisphere values, which are in the range (270,360)
    if rlatE >= 270:
        rlatE -= 360
    if rlatO >= 270:
        rlatO -= 360

    # Check to see that the latitude is in range: -90 .. +90
    if rlatE < -90 or rlatE > 90 or rlatO < -90 or rlatO > 90:
        raise ValueError('latitude out of range')

    # Find latitude zone, abort if the two positions are not in the same zone
    nl = NL(rlatE)
    if nl != NL(rlatO):
        raise ValueError('messages lie in different latitude zones')

    # Compute n(i)
    nE = nl
    nO = max(1, nl - 1)

    # Compute the Longitude Index "m"
    m = math.floor((((lonE * (nl - 1)) - (lonO * nl)) / 131072.0) + 0.5)

    # Compute global longitudes
    rlonE = (360.0 / nE) * (MOD(m, nE) + lonE / 131072.0)
    rlonO = (360.0 / nO) * (MOD(m, nO) + lonO / 131072.0)

    # Renormalize to -180 .. +180
    rlonE -= math.floor((rlonE + 180) / 360) * 360
    rlonO -= math.floor((rlonO + 180) / 360) * 360

    return (rlatE, rlonE, rlatO, rlonO)
