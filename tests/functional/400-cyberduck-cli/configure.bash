#!/bin/bash
yum -y -q clean all
#install cyberduck cli 
echo -e "[duck-stable]\n\
name=duck-stable\n\
baseurl=https://repo.cyberduck.io/stable/\$basearch/\n\
enabled=1\n\
gpgcheck=0" |  tee /etc/yum.repos.d/duck-stable.repo > /dev/null
yum -y -q install duck
mkdir -p /home/ironman/.duck/profiles
sed -i "s/IP/${IP}/g" S3\ \(HTTP\).cyberduckprofile
cp S3\ \(HTTP\).cyberduckprofile  /home/ironman/.duck/profiles
#install aws cli to create the bucket in which cyber duck will work
yum -y -q install python-pip openssl openssl-devel libffi libffi-devel wget
pip install pyopenssl ndg-httpsclient pyasn1
pip install awscli

#shunit is not yet in the standard repo
wget http://mirrors.karan.org/epel7/Packages/shunit2/20131231015457/2.1.6-3.el6.x86_64/shunit2-2.1.6-3.el7.noarch.rpm
yum -y -q localinstall  ./shunit2-2.1.6-3.el7.noarch.rpm 

