#!/bin/bash
#
# Hadoop Cluster start-up and admin operations
# Author:    Michael Kepple
# Called by: ~/.bash_profile
# Date:      29 Feb 2014
#
source ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-i386
export HADOOP_INSTALL=/home/hadoop/hadoop-1.2.1
export PATH=$PATH:$HADOOP_INSTALL/bin:$JAVA_HOME/bin
HADOOP_VERSION="hadoop-1.2.1"
HBASE_VERSION="hbase-0.94.8"
ZOOKEEPER_VERSION="zookeeper-3.4.5"
HOSTNAME='echo $HOSTNAME | sed 's/\..*//''
NODES=('nano' 'ammo' 'spino' 'techno' 'dryo' 'grypo' 'seismo' 'anon' 'rhino' 'aho' 'tito' 'tino' 'hippo' 'specko' 'muto' 'omego');
CLASS_ACER=('nano' 'ammo' 'spino' 'techno' 'dryo' 'grypo' 'seismo' 'anon');
sshAgentInfo=$HOME/.ssh/agentInfo

node_sync()
{
    CLASS_NAME="${2}[@]"
    CLASS_ARRAY=( "${!CLASS_NAME}" );
    for node in ${CLASS_ARRAY[@]}
    do
        if [ "$node" = "$HOSTNAME" ]; then
            continue
        fi
        rsync -avz $HOME/$2/$1/conf $node:~/$1
    done
}

node_delete()
{
    for node in ${NODES[@]}
    do
        ssh $node "rm -rf $1"
    done
}

while getopts "h:d:z:x:" opt; do
    case $opt in
        x) node_delete $OPTARG
	   ;;
        h) node_sync $HADOOP_VERSION $OPTARG
           ;;
        d) echo "Hbase -> " ${ACER_CLASS[*]}
	   #node_sync $HBASE_VERSION
           ;;
        z) echo "Zookeeper -> "
	   #node_sync $ZOOKEEPER_VERSION
    esac
done

# will exist if agent is already up - load PID, etc.
if [ -e $sshAgentInfo ]
then
    source $sshAgentInfo
fi

ssh-add -l > /dev/null
# $? indicates the error code of the last executed command - ssh-agent isn't up.
if [ $? != 0 ]
then
    # start is and store it's output to file to be sourced on other login's.
    ssh-agent -s | sed 's/^echo/#echo/' > $sshAgentInfo
    source $sshAgentInfo
    ssh-add
fi
