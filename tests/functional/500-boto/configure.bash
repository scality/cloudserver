#!/bin/bash
yum -y -q clean all
#install python
yum -y -q install python python-pip
pip install pytest
