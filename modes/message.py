# -*- mode: python; indent-tabs-mode: nil -*-


__all__ = ('ESType', 'decode', 'decode_aa')

from enum import Enum

from . import altitude
from . import squawk
from . import crc

ais_charset = " ABCDEFGHIJKLMNOPQRSTUVWXYZ????? ???????????????0123456789??????"


class DF0:
    def __init__(self, frombuf):
        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.VS = (frombuf[0] & 0x04) >> 2  # 1 bit
        self.CC = (frombuf[0] & 0x02) >> 1  # 1 bit
        # 1 bit pad
        self.SL = (frombuf[1] & 0xe0) >> 5  # 3 bits
        # 2 bits pad
        self.RI = ((frombuf[1] & 0x03) << 1) | ((frombuf[2] & 0x80) >> 7)  # 4 bits
        # 2 bits pad
        self.AC = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        # 24 bits A/P

        self.squawk = self.callsign = None
        self.altitude = altitude.decode_ac13(self.AC)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class DF4:
    def __init__(self, frombuf):
        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.FS = (frombuf[0] & 0x07)       # 3 bits
        self.DR = (frombuf[1] & 0xf8) >> 3  # 5 bits
        self.UM = ((frombuf[1] & 0x07) << 3) | ((frombuf[2] & 0xe0) >> 5)  # 6 bits
        self.AC = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        # 24 bits A/P

        self.squawk = self.callsign = None
        self.altitude = altitude.decode_ac13(self.AC)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class DF5:
    def __init__(self, frombuf):
        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.FS = (frombuf[0] & 0x07)       # 3 bits
        self.DR = (frombuf[1] & 0xf8) >> 3  # 5 bits
        self.UM = ((frombuf[1] & 0x07) << 3) | ((frombuf[2] & 0xe0) >> 5)  # 6 bits
        self.ID = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        # 24 bits A/P

        self.altitude = self.callsign = None
        self.squawk = squawk.decode_id13(self.ID)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class DF11:
    def __init__(self, frombuf):
        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.CA = (frombuf[0] & 0x07)       # 3 bits
        self.AA = (frombuf[1] << 16) | (frombuf[2] << 8) | frombuf[3]  # 24 bits
        # 24 bits P/I

        self.squawk = self.callsign = self.altitude = None

        r = crc.residual(frombuf)
        if r == 0:
            self.crc_ok = True
        elif (r & ~0x7f) == 0:
            self.crc_ok = None
        else:
            self.crc_ok = False
        self.address = self.AA


class DF16:
    def __init__(self, frombuf):
        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.VS = (frombuf[0] & 0x04) >> 2  # 1 bit
        # 2 bits pad
        self.SL = (frombuf[1] & 0xe0) >> 5  # 3 bits
        # 2 bits pad
        self.RI = ((frombuf[1] & 0x03) << 1) | ((frombuf[2] & 0x80) >> 7)  # 4 bits
        # 2 bits pad
        self.AC = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        self.MV = frombuf[4:11]  # 56 bits
        # 24 bits A/P

        self.squawk = self.callsign = None
        self.altitude = altitude.decode_ac13(self.AC)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class CommB(object):
    def __init__(self, frombuf):
        self.MB = frombuf[4:11]  # 56 bits

        if frombuf[4] != 0x20:
            self.callsign = None
        else:
            callsign = (
                ais_charset[(frombuf[5] & 0xfc) >> 2] +
                ais_charset[((frombuf[5] & 0x02) << 4) | ((frombuf[6] & 0xf0) >> 4)] +
                ais_charset[((frombuf[6] & 0x0f) << 2) | ((frombuf[7] & 0xc0) >> 6)] +
                ais_charset[frombuf[7] & 0x3f] +
                ais_charset[(frombuf[8] & 0xfc) >> 2] +
                ais_charset[((frombuf[8] & 0x02) << 4) | ((frombuf[9] & 0xf0) >> 4)] +
                ais_charset[((frombuf[9] & 0x0f) << 2) | ((frombuf[10] & 0xc0) >> 6)] +
                ais_charset[frombuf[10] & 0x3f]
            )

            if callsign != '        ' and callsign.find('?') == -1:
                self.callsign = callsign
            else:
                self.callsign = None


class DF20(CommB):
    def __init__(self, frombuf):
        CommB.__init__(self, frombuf)

        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.FS = (frombuf[0] & 0x07)       # 3 bits
        self.DR = (frombuf[1] & 0xf8) >> 3  # 5 bits
        self.UM = ((frombuf[1] & 0x07) << 3) | ((frombuf[2] & 0xe0) >> 5)  # 6 bits
        self.AC = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        # 56 bits MB
        # 24 bits A/P

        self.squawk = None
        self.altitude = altitude.decode_ac13(self.AC)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class DF21(CommB):
    def __init__(self, frombuf):
        CommB.__init__(self, frombuf)

        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.FS = (frombuf[0] & 0x07)       # 3 bits
        self.DR = (frombuf[1] & 0xf8) >> 3  # 5 bits
        self.UM = ((frombuf[1] & 0x07) << 3) | ((frombuf[2] & 0xe0) >> 5)  # 6 bits
        self.ID = ((frombuf[2] & 0x1f) << 8) | frombuf[3]  # 13 bits
        # 56 bits MB
        # 24 bits A/P

        self.altitude = None
        self.squawk = squawk.decode_id13(self.ID)
        self.crc_ok = None
        self.address = crc.residual(frombuf)


class ESType(Enum):
    id_and_category = 1
    airborne_position = 2
    surface_position = 3
    airborne_velocity = 4
    other = 5

es_types = {
    0: (ESType.airborne_position, 0),
    1: (ESType.id_and_category, None),
    2: (ESType.id_and_category, None),
    3: (ESType.id_and_category, None),
    4: (ESType.id_and_category, None),
    5: (ESType.surface_position, 9),
    6: (ESType.surface_position, 8),
    7: (ESType.surface_position, 7),
    8: (ESType.surface_position, 6),
    9: (ESType.airborne_position, 9),
    10: (ESType.airborne_position, 8),
    11: (ESType.airborne_position, 7),
    12: (ESType.airborne_position, 6),
    13: (ESType.airborne_position, 5),
    14: (ESType.airborne_position, 4),
    15: (ESType.airborne_position, 3),
    16: (ESType.airborne_position, 2),
    17: (ESType.airborne_position, 1),
    18: (ESType.airborne_position, 0),
    19: (ESType.airborne_velocity, None),
    20: (ESType.airborne_position, 9),
    21: (ESType.airborne_position, 8),
    22: (ESType.airborne_position, 0)
}


class ExtendedSquitter(object):
    def __init__(self, frombuf):
        metype = (frombuf[4] & 0xf8) >> 3
        self.estype, self.nuc = es_types.get(metype, (ESType.other, None))

        if self.estype is ESType.airborne_position:
            self.SS = (frombuf[4] & 0x06) >> 1
            self.SAF = frombuf[4] & 0x01
            self.AC12 = (frombuf[5] << 4) | ((frombuf[6] & 0xf0) >> 4)
            self.T = (frombuf[6] & 0x08) >> 3
            self.F = (frombuf[6] & 0x04) >> 2
            self.LAT = (((frombuf[6] & 0x03) << 15) |
                        (frombuf[7] << 7) |
                        ((frombuf[8] & 0xfe) >> 1))
            self.LON = (((frombuf[8] & 0x01) << 16) |
                        (frombuf[9] << 8) |
                        frombuf[10])
            self.altitude = altitude.decode_ac12(self.AC12)
            self.callsign = None

        elif self.estype is ESType.id_and_category:
            self.CATEGORY = frombuf[4] & 0x07
            self.altitude = None
            self.callsign = (
                ais_charset[(frombuf[5] & 0xfc) >> 2] +
                ais_charset[((frombuf[5] & 0x02) << 4) | ((frombuf[6] & 0xf0) >> 4)] +
                ais_charset[((frombuf[6] & 0x0f) << 2) | ((frombuf[7] & 0xc0) >> 6)] +
                ais_charset[frombuf[7] & 0x3f] +
                ais_charset[(frombuf[8] & 0xfc) >> 2] +
                ais_charset[((frombuf[8] & 0x02) << 4) | ((frombuf[9] & 0xf0) >> 4)] +
                ais_charset[((frombuf[9] & 0x0f) << 2) | ((frombuf[10] & 0xc0) >> 6)] +
                ais_charset[frombuf[10] & 0x3f]
            )

        else:
            self.altitude = None
            self.callsign = None


class DF17(ExtendedSquitter):
    def __init__(self, frombuf):
        ExtendedSquitter.__init__(self, frombuf)

        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.CA = (frombuf[0] & 0x07)       # 3 bits
        self.AA = (frombuf[1] << 16) | (frombuf[2] << 8) | frombuf[3]  # 24 bits
        # 56 bits ME
        # 24 bits CRC

        self.squawk = None
        self.crc_ok = (crc.residual(frombuf) == 0)
        self.address = self.AA


class DF18(ExtendedSquitter):
    def __init__(self, frombuf):
        ExtendedSquitter.__init__(self, frombuf)

        self.DF = (frombuf[0] & 0xf8) >> 3  # 5 bits
        self.CF = (frombuf[0] & 0x07)       # 3 bits
        self.AA = (frombuf[1] << 16) | (frombuf[2] << 8) | frombuf[3]  # 24 bits
        # 56 bits ME
        # 24 bits CRC

        self.squawk = None
        self.crc_ok = (crc.residual(frombuf) == 0)
        self.address = self.AA


message_types = {
    0: DF0,
    4: DF4,
    5: DF5,
    11: DF11,
    16: DF16,
    17: DF17,
    18: DF18,
    20: DF20,
    21: DF21
}


def decode(frombuf):
    df = (frombuf[0] & 0xf8) >> 3
    try:
        return message_types[df](frombuf)
    except KeyError:
        return None


def decode_aa(frombuf):
    return (frombuf[1] << 16) | (frombuf[2] << 8) | frombuf[3]
