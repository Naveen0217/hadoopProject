#!/bin/bash
#
# Hadoop installer for slave nodes.
# Author:    Michael Kepple
# Date:      5 Mar 2014
#
scp newo:/etc/hosts /etc/hosts
scp newo:/home/hadoop/.ssh/authorized_keys /home/hadoop/.ssh/authorized_keys
scp newo:/home/hadoop/.bash_profile /home/hadoop/.bash_profile
yum install wget
wget . http://apt.sw.be/redhat/el6/en/x86_64/rpmforge/RPMS/rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm
rpm -ivh rpmforge-release-0.5.3-1.el6.rf.x86_64.rpm
yum install ganglia ganglia-gmond
service gmond start
wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%24www.oracle.com" "http://download.oracle.com/otn-pub/java/jdk/7u51-b13/jdk-7u51-linux-x64.rpm"
rpm -ivh jdk-7u51-linux-x64.rpm
wget -O /etc/yum.repos.d/bigtop.repo http://www.apache.org/dist/bigtop/bigtop-0.7.0/repos/centos6/bigtop.repo
yum install hadoop\*
yum remove hadoop-yarn-resourcemanager
yum remove hadoop-hdfs-secondarynamenode
yum remove hadoop-yarn-proxyserver
yum remove hadoop-hdfs-namenode
rm -rf /etc/hadoop/conf
scp -r newo:/etc/hadoop/conf /etc/hadoop/conf
mkdir -p /home/hdfs/dfs/data
chown -R hdfs /home/hdfs
mkdir -p /home/hdfs/yarn
chown -R yarn /home/hdfs/yarn
service hadoop-hdfs-datanode start
service hadoop-yarn-nodemanager start
