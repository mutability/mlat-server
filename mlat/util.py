# -*- mode: python; indent-tabs-mode: nil -*-

import random


def fuzzy(t):
    return round(random.uniform(0.9*t, 1.1*t), 0)
