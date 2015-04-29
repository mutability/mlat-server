# -*- mode: python; indent-tabs-mode: nil -*-

import os
import json
import sys
import numpy
import tempfile
import subprocess

from contextlib import closing

import mlat.geodesy
import mlat.constants
import mlat.kalman


class DummyReceiver(object):
    def __init__(self, position):
        self.position = position


def load_data(f, icao):
    data = []
    line = f.readline()
    while line:
        try:
            state = json.loads(line)
        except ValueError:
            print('skipped: ' + line)
        finally:
            line = f.readline()

        if int(state['icao'], 16) != icao:
            continue

        timestamp = state['time']
        altitude = state['altitude']
        distinct = state['distinct']
        ecef = numpy.array(state['ecef'])
        ecef_cov = numpy.array(state['ecef_cov']).reshape((3, 3))
        cluster = [(DummyReceiver((x, y, z)), t/1e6, v/1e12) for x, y, z, t, v in state['cluster']]

        data.append((timestamp, cluster, altitude, ecef, ecef_cov, distinct))

    return data


def run_filter(data, filterstate, outpng):
    with closing(tempfile.NamedTemporaryFile(mode='wt', prefix='raw_', suffix='.tsv', delete=False)) as raw_tsv:
        with closing(tempfile.NamedTemporaryFile(mode='wt', prefix='filter_', suffix='.tsv', delete=False)) as filter_tsv:
            for timestamp, cluster, altitude, ecef, ecef_cov, distinct in data:
                llh = mlat.geodesy.ecef2llh(ecef)
                print('{t}\t{llh[0]:.4f}\t{llh[1]:.4f}\t{llh[2]:.0f}'.format(t=timestamp, llh=llh), file=raw_tsv)
                filterstate.update(timestamp, cluster, altitude, ecef, ecef_cov, distinct)
                if filterstate.position is not None:
                    print('{t}\t{llh[0]:.4f}\t{llh[1]:.4f}\t{llh[2]:.0f}\t{speed:.0f}\t{pe:.0f}\t{ve:.0f}'.format(
                        t=timestamp,
                        llh=filterstate.position_llh,
                        speed=filterstate.ground_speed,
                        pe=filterstate.position_error,
                        ve=filterstate.velocity_error),
                          file=filter_tsv)

    with closing(tempfile.NamedTemporaryFile(mode='wt', prefix='gnuplot_', suffix='.cmd', delete=False)) as gnuplot_script:
        print("""
set terminal png size 800,800;
set output "{outpng}";

plot "{raw_tsv.name}" using 3:2 title "least-squares", "{filter_tsv.name}" using 3:2 with lines lt 3 title "Kalman";

set output "err_{outpng}";
plot [:] [0:2000] "{filter_tsv.name}" using 1:6 with lines title "position error", "" using 1:7  with lines lt 3 title "velocity error";

set output "speed_{outpng}";
plot [:] [0:] "{filter_tsv.name}" using 1:5 with lines title "groundspeed";
""".format(outpng=outpng,
           raw_tsv=raw_tsv,
           filter_tsv=filter_tsv),
              file=gnuplot_script)

    subprocess.check_call(["gnuplot", gnuplot_script.name])

    os.unlink(gnuplot_script.name)
    os.unlink(filter_tsv.name)
    os.unlink(raw_tsv.name)

if __name__ == '__main__':
    icao = int(sys.argv[1], 16)
    with closing(open('pseudoranges.json')) as f:
        data = load_data(f, icao)

    for pn in (0.01, 0.02, 0.04, 0.06, 0.08, 0.10, 0.12, 0.14, 0.16, 0.18, 0.20):
        print(pn)

        filt = mlat.kalman.KalmanStateCA(icao)
        filt.process_noise = pn
        filt.min_tracking_receivers = 4
        filt.outlier_mahalanobis_distance = 10.0
        run_filter(data, filt, "kalman_ca_{pn:.2f}.png".format(pn=pn))

        filt = mlat.kalman.KalmanStateCV(icao)
        filt.process_noise = pn
        filt.min_tracking_receivers = 4
        filt.outlier_mahalanobis_distance = 10.0
        run_filter(data, filt, "kalman_cv_{pn:.2f}.png".format(pn=pn))
