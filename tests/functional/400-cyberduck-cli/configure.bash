#!/bin/bash
#install cyberduck cli 
echo "deb https://s3.amazonaws.com/repo.deb.cyberduck.io stable main" >> /etc/apt/sources.list
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys FE7097963FEFBE72
apt-get update
apt-get install -y -q duck

mkdir -p /home/ironman/.duck/profiles
sed -i "s/IP/${IP}/g" S3\ \(HTTP\).cyberduckprofile
cp S3\ \(HTTP\).cyberduckprofile  /home/ironman/.duck/profiles

#install aws cli to create the bucket in which cyber duck will work
pip install awscli

#shunit is not yet in the standard repo
apt-get install -y -q shunit2
