#!/bin/bash
apt-get install -y -q  python-dateutil python-magic
wget http://launchpadlibrarian.net/222422124/s3cmd_1.6.0-2_all.deb
dpkg -i s3cmd*.deb
