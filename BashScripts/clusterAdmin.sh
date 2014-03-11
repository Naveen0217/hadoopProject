#!/bin/bash
#
# Hadoop Cluster start-up and admin operations
# Author:      Michael Kepple
# Called by:   ~/.bash_profile
# Date:        29 Feb 2014
# Note:        Needs updating.
#
source ~/.bashrc
export JAVA_HOME=/usr/java/jdk1.7.0_51
export HADOOP_INSTALL=/home/hadoop/hadoop-1.2.1
export PATH=$PATH:$HADOOP_INSTALL/bin:$JAVA_HOME/bin
HADOOP_VERSION="hadoop-1.2.1"
HBASE_VERSION="hbase-0.94.8"
ZOOKEEPER_VERSION="zookeeper-3.4.5"
HOSTNAME='echo $HOSTNAME | sed 's/\..*//''
NODES=('aho' 'tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
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

confDiff()
{
    echo "I'm a stub."
}

# Should be run as root/sudo'd
# Note: master node must have installed sshpass
admin_sync()
{
    for node in ${NODES[@]}
    do
        sshpass -p $1 scp /etc/hosts $node:/etc/hosts
        sshpass -p $1 scp /home/hadoop/.ssh/authorized_keys $node:/home/hadoop/.ssh/authorized_keys
	sshpass -p $1 ssh $node -t chown hadoop /home/hadoop/.ssh/authorized_keys
	sshpass -p $1 scp /home/hadoop/.bash_profile $node:/home/hadoop/.bash_profile
	sshpass -p $1 scp /home/hadoop/.bashrc $node:/home/hadoop/.bashrc
	sshpass -p $1 scp /home/hadoop/clusterAdmin.sh $node:/home/hadoop/clusterAdmin.sh
    done
}

# Note: master node must have installed expect.
init_passphrases()
{
    for node in ${NODES[@]}
    do
        /usr/bin/expect -f ./clusterExpect $node $1
    done
}	

while getopts "h:d:z:x:s:i:" opt; do
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
	   ;;
        i) init_passphrases $OPTARG
           ;;
        s) admin_sync $OPTARG
	   ;;
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
