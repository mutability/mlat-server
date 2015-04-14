# -*- mode: python; indent-tabs-mode: nil -*-


from .altitude import decode_ac12, decode_ac13
from .squawk import decode_id13


class ModeSMessage:
    """A single Mode S message of some kind."""

    def __init__(self, df, crc_ok,
                 aa=None, ac=None, af=None, ca=None, cc=None,
                 cf=None, dr=None, fs=None, ident=None, mb=None,
                 me=None, mv=None, ri=None, sl=None, um=None, vs=None,
                 _skip1=None, _skip2=None, _skip3=None,
                 possible_address=None, possible_callsign=None):
        self.df = df
        self.aa = aa
        self.ac = ac
        self.af = af
        self.ca = ca
        self.cc = cc
        self.cf = cf
        self.dr = dr
        self.fs = fs
        self.ident = ident
        self.mb = mb
        self.me = me
        self.mv = mv
        self.ri = ri
        self.sl = sl
        self.um = um
        self.vs = vs
        self.crc_ok = crc_ok
        self.possible_address = possible_address
        self.possible_callsign = possible_callsign

    @property
    def address(self):
        if self.aa:
            return self.aa
        else:
            return self.possible_address

    @property
    def altitude(self):
        if self.ac:
            return decode_ac13(self.ac)
        elif self.me and self.me.ac12:
            return decode_ac12(self.me.ac12)
        else:
            return None

    @property
    def squawk(self):
        if self.ident:
            return decode_id13(self.ident)
        else:
            return None

    @property
    def callsign(self):
        if self.me and self.me.callsign:
            return self.me.callsign
        else:
            return self.possible_callsign


def check_pi(crc):
    """See if this CRC residual is OK as a Parity/Interrogator residual.

    Returns True if it's definitely OK, False if it's definitely bad,
    or None if it's indeterminate (might be a valid non-zero interrogator,
    but might also be transmission errors)."""

    if crc == 0:
        return True
    if (crc & ~0x7f) == 0:
        return None
    return False
