#!/bin/bash
yum -y -q clean all
yum -y -q install python-pip openssl openssl-devel libffi libffi-devel wget diffutils
pip install pyopenssl ndg-httpsclient pyasn1
pip install awscli
#shunit is not yet in the standard repo
wget http://mirrors.karan.org/epel7/Packages/shunit2/20131231015457/2.1.6-3.el6.x86_64/shunit2-2.1.6-3.el7.noarch.rpm
yum -y -q localinstall  ./shunit2-2.1.6-3.el7.noarch.rpm
