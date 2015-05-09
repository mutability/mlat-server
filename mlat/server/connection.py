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
Connection interface description.
"""


class Connection(object):
    """Interface for receiver connections.

    A receiver connection is something that can send messages (filter requests,
    multilateration results) to a particular receiver. A single connection
    may handle only a single receiver, or may multiplex multiple receivers.

    This is a duck-typed interface, implementations are not required to inherit
    this class as long as they provide methods with equivalent signatures.
    """

    def request_traffic(self, receiver, icao_set):
        """Request that a receiver starts sending traffic for exactly
        the given set of aircraft only.

        receiver: the handle of the concerned receiver
        icao_set: a set of ICAO addresses (as ints) to send (_not_ copied, don't modify afterwards!)
        """
        raise NotImplementedError

    def report_mlat_position(self, receiver,
                             receive_timestamp, address, ecef, ecef_cov, receivers, distinct):
        """Report a multilaterated position result.

        receiver: the handle of the concerned receiver
        receive_timestamp: the approx UTC time of the position
        address: the ICAO address of the aircraft (as an int)
        ecef: an (x,y,z) tuple giving the position in ECEF coordinates
        ecef_cov: a 3x3 matrix giving the covariance matrix of ecef
        receivers: the set of receivers that contributed to the result
        distinct: the number of distinct receivers (<= len(receivers))
        """
        raise NotImplementedError
