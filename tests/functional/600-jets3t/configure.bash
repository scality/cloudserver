#!/bin/bash
yum -y -q clean all
#install java 
yum -y -q install java-1.7.0-openjdk java-1.7.0-openjdk-devel
#install maven 
yum -y -q install wget
wget http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz
tar xzf apache-maven-3.0.5-bin.tar.gz -C /usr/local
cd /usr/local
ln -s apache-maven-3.0.5 maven