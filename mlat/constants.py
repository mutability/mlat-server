# -*- mode: python; indent-tabs-mode: nil -*-

import math

# signal propagation speed in metres per second
Cair = 299792458 / 1.0003

# degrees to radians
DTOR = math.pi / 180.0
# radians to degrees
RTOD = 180.0 / math.pi

# feet to metres
FTOM = 0.3038
# metres to feet
MTOF = 1.0/FTOM

# m/s to knots
MS_TO_KTS = 1.9438

# m/s to fpm
MS_TO_FPM = MTOF * 60
