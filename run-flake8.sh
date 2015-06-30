#!/bin/sh
D=`dirname $0`
python3 /usr/lib/python3/dist-packages/flake8/run.py --exclude=.git,__pycache__,$D/modes/altitude_lookup.py,tools,test mlat-server $D
