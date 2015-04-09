# -*- mode: python; indent-tabs-mode: nil -*-

import math

# clock frequency for timing
XTAL = 12000000

# signal propagation speed in metres per clockcycle
Cair = 299792458 / 1.0003 / XTAL

# degrees to radians
DTOR = math.pi / 180.0
# radians to degrees
RTOD = 180.0 / math.pi

# feet to metres
FTOM = 0.3038
# metres to feet
MTOF = 1.0/FTOM

# absolute maximum receiver range, metres
MAX_RANGE = 500e3

# absolute maximum altitude, metres
MAX_ALT = 50000 * FTOM
