#!/bin/bash
yum -y -q clean all
yum -y -q install s3cmd
yum -y -q install wget
#shunit is not yet in the standard repo
wget http://mirrors.karan.org/epel7/Packages/shunit2/20131231015457/2.1.6-3.el6.x86_64/shunit2-2.1.6-3.el7.noarch.rpm
yum -y -q localinstall  ./shunit2-2.1.6-3.el7.noarch.rpm
yum -y -q install socat
nohup socat TCP-LISTEN:80,fork TCP:127.0.0.1:8000 > /dev/null 2>&1 &
exit 0
