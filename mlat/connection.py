# -*- mode: python; indent-tabs-mode: nil -*-


class Connection(object):
    """Interface for receiver connections.

    A receiver connection is something that can send messages (filter requests,
    multilateration results) to a particular receiver. A single connection
    may handle only a single receiver, or may multiplex multiple receivers.

    This is a duck-typed interface, implementations are not required to inherit
    this class as long as they provide methods with equivalent signatures.
    """

    def request_traffic(self, receiver, icao):
        """Request that a receiver starts sending traffic for the given
        aircraft.

        receiver: the handle of the concerned receiver
        icao: an ICAO addresses (as int) to start sending
        """
        raise NotImplementedError

    def suppress_traffic(self, receiver, icao):
        """Request that a receiver stops sending traffic for the given
        aircraft.

        receiver: the handle of the concerned receiver
        icao: an ICAO addresses (as int) to stop sending
        """
        raise NotImplementedError

    def report_mlat_position(self, receiver,
                             icao, utc, ecef, ecef_cov, nstations):
        """Report a multilaterated position result.

        receiver: the handle of the concerned receiver
        icao: the ICAO address of the aircraft (as an int)
        utc: the approximate validity time of the position
        ecef: an (x,y,z) tuple giving the position in ECEF coordinates
        ecef_cov: a 3x3 matrix giving the covariance matrix of ecef
        nstations: the number of stations that contributed to the result
        """
        raise NotImplementedError
