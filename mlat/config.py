# -*- mode: python; indent-tabs-mode: nil -*-

import mlat.constants

# minimum NUCp value to accept as a sync message
MIN_NUC = 6

# absolute maximum receiver range for sync messages, metres
MAX_RANGE = 500e3

# maximum distance between even/odd DF17 messages, metres
MAX_INTERMESSAGE_RANGE = 10e3

# absolute maximum altitude, metres
MAX_ALT = 50000 * mlat.constants.FTOM

# target sync rate, syncs/second
TARGET_SYNC_RATE = 1.0
