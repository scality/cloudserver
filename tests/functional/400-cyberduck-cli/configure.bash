#!/bin/bash
#install cyberduck cli 
wget https://dist.duck.sh/duck-4.8.18509.amd64.deb
dpkg  -i duck-4.8.18509.amd64.deb  

mkdir -p /home/ironman/.duck/profiles
sed -i "s/IP/${IP}/g" S3\ \(HTTP\).cyberduckprofile
cp S3\ \(HTTP\).cyberduckprofile  /home/ironman/.duck/profiles

#install aws cli to create the bucket in which cyber duck will work
pip install awscli

#shunit is not yet in the standard repo
apt-get install -y -q shunit2
