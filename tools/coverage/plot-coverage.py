#!/usr/bin/python2

import cairo, colorsys
import csv, math, json, time, sys, gzip
from contextlib import closing

class StationCoverage:
    def __init__(self, name, lat, lon, privacy=False, binsize=0.05, alt_mode=False, is_station=True):
        self.name = name        
        self.alt_mode = alt_mode
        if lat:
            if privacy:
                self.station_lat = self.station_lon = None
            else:
                self.station_lat = round(lat / (binsize/2)) * (binsize/2)
                self.station_lon = round(lon / (binsize/2)) * (binsize/2)
        else:
            self.station_lat = self.station_lon = None

        self.binsize = binsize
        self.bins = {}
        self.min_lat = self.max_lat = self.min_lon = self.max_lon = None
        self.max_count = None
        self.is_station = is_station

    def add_position(self, lat, lon, alt, err_est):
        bin_lat = math.floor(lat / self.binsize) * self.binsize
        bin_lon = math.floor(lon / self.binsize) * self.binsize

        if self.min_lat is None or bin_lat < self.min_lat: self.min_lat = bin_lat
        if self.min_lon is None or bin_lon < self.min_lon: self.min_lon = bin_lon
        if self.max_lat is None or bin_lat > self.max_lat: self.max_lat = bin_lat
        if self.max_lon is None or bin_lon > self.max_lon: self.max_lon = bin_lon

        bin_key = (bin_lat, bin_lon)
        if self.alt_mode:
            data = self.bins.setdefault(bin_key, [1,99999.0])
            data[1] = min(data[1], alt)            
        else:
            data = self.bins.setdefault(bin_key, [0,0.0])
            data[0] += 1
            data[1] += err_est
            if self.max_count is None or data[0] > self.max_count: self.max_count = data[0]
        
    def write(self, basedir, pngfile, metafile, pixels_per_degree=None):
        if len(self.bins) == 0: return

        if not pixels_per_degree: pixels_per_degree = math.ceil(4.0 / self.binsize)

        min_lon = self.min_lon
        min_lat = self.min_lat
        max_lat = self.max_lat + self.binsize
        max_lon = self.max_lon + self.binsize
        binsize = self.binsize

        lon_span = (max_lon-min_lon)
        lat_span = (max_lat-min_lat)
        xsize = int(math.ceil(lon_span*pixels_per_degree))
        ysize = int(math.ceil(lat_span*pixels_per_degree))

        surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, xsize, ysize)

        cc = cairo.Context(surface)

        cc.set_operator(cairo.OPERATOR_SOURCE)
        cc.set_antialias(cairo.ANTIALIAS_NONE)
        cc.scale(1.0 * xsize / lon_span, -1.0 * ysize / lat_span)
        cc.translate(-min_lon, -max_lat)

        # transparent background
        cc.set_source(cairo.SolidPattern(0,0,0,0))
        cc.paint()

        # draw bins
        for (bin_lat,bin_lon), (count,val) in self.bins.items():
            a = 0.75

            if self.alt_mode:
                if val < 2000:
                    hue = 20
                elif val < 10000:
                    hue = 20 + 120.0 * (val-2000) / 8000.0
                elif val < 40000:
                    hue = 140 + 160 * (val-10000) / 30000.0
                else:
                    hue = 300
            else:
                err_est = val / count
                hue = 150.0 - 150 * ((err_est / 5000.0)**0.5)
                if hue < 0:
                    hue = 0

            r,g,b = colorsys.hls_to_rgb(hue/360.0, 0.5, 1.0)
            cc.set_source(cairo.SolidPattern(r,g,b,a))

            cc.move_to(bin_lon, bin_lat)
            cc.line_to(bin_lon + binsize, bin_lat)
            cc.line_to(bin_lon + binsize, bin_lat + binsize)
            cc.line_to(bin_lon, bin_lat + binsize)
            cc.close_path()
            cc.fill()

        surface.write_to_png(basedir + '/' + pngfile)
        
        print >>metafile, """
coverage['{name}'] = {{
  name:    '{name}',
  lat:     {station_lat},
  lon:     {station_lon},
  min_lat: {min_lat},
  min_lon: {min_lon},
  max_lat: {max_lat},
  max_lon: {max_lon},
  image:   '{pngfile}',
  is_station: {is_station}
}};""".format(name = self.name,
              station_lat = (self.station_lat is None) and 'null' or self.station_lat,
              station_lon = (self.station_lon is None) and 'null' or self.station_lon,
              min_lat = self.min_lat,
              min_lon = self.min_lon,
              max_lat = max_lat,
              max_lon = max_lon,
              pngfile = pngfile,
              is_station = self.is_station and "true" or "false")

def multiopen(path):
    if path[-3:] == '.gz':
        return gzip.open(path, 'rb')
    else:
        return open(path, 'rb')

def plot_from_datafile(csvfile, jsonfile, outdir):
    station_coverage = {
        '*' : StationCoverage('all', None, None, is_station=False),
        '4+' : StationCoverage('4plus', None, None, is_station=False),
        '5+' : StationCoverage('5plus', None, None, is_station=False),
        '6+' : StationCoverage('6plus', None, None, is_station=False),
        '10000-' : StationCoverage('below10000', None, None, is_station=False),
        '18000-' : StationCoverage('below18000', None, None, is_station=False),
        'byalt' : StationCoverage('byalt', None, None, alt_mode=True, is_station=False)
    }

    with closing(multiopen(jsonfile)) as f:
        station_data = json.load(f)

    for station_name, station_pos in station_data.items():
        station_coverage[station_name] = StationCoverage(station_name, station_pos['lat'], station_pos['lon'], privacy=station_pos['privacy'])

    first = last = None            
    num_positions = 0
    with closing(multiopen(csvfile)) as f:
        reader = csv.reader(f)
        for row in reader:
            try:
                t,addr,callsign,squawk,lat,lon,alt,err_est,nstations,ndistinct,stationlist = row[:11]
            except ValueError as e:
                print 'row', reader.line_num, 'failed: ', str(e)
                print repr(row)
                continue

            t = float(t)
            lat = float(lat)
            lon = float(lon)
            alt = float(alt)
            err_est = max(0, float(err_est))
            nstations = int(nstations)
            ndistinct = int(ndistinct)

            if not first:
                first = last = t
            else:
                first = min(first,t)
                last = max(last,t)

            station_coverage['*'].add_position(lat,lon,alt,err_est)
            station_coverage['byalt'].add_position(lat,lon,alt,err_est)
            if nstations >= 4:
                station_coverage['4+'].add_position(lat,lon,alt,err_est)
            if nstations >= 5:
                station_coverage['5+'].add_position(lat,lon,alt,err_est)
            if nstations >= 6:
                station_coverage['6+'].add_position(lat,lon,alt,err_est)
            if alt <= 10000:
                station_coverage['10000-'].add_position(lat,lon,alt,err_est)
            if alt <= 18000:
                station_coverage['18000-'].add_position(lat,lon,alt,err_est)

            for s in stationlist.split(','):
                sc = station_coverage.get(s)
                if not sc:
                    sc = station_coverage[s] = StationCoverage(s, None, None)
                sc.add_position(lat, lon, alt, err_est)

            num_positions += 1

    with closing(open(outdir + '/data.js', 'w')) as metafile:
        print >>metafile, "var first_position = '{d}';".format(d = time.strftime("%Y/%m/%d %H:%M:%S UTC", time.gmtime(first)))
        print >>metafile, "var last_position = '{d}';".format(d = time.strftime("%Y/%m/%d %H:%M:%S UTC", time.gmtime(last)))
        print >>metafile, "var num_positions = {n};".format(n = num_positions)
        print >>metafile, "var coverage = {};"
        for sc in station_coverage.values():
            pngfile = 'coverage_{n}.png'.format(n=sc.name)
            #print 'Writing', pngfile
            sc.write(basedir = outdir, pngfile = pngfile, metafile = metafile)

if __name__ == '__main__':
    plot_from_datafile(csvfile=sys.argv[1], jsonfile=sys.argv[2], outdir=sys.argv[3])
