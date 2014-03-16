#!/bin/bash
#
# Hadoop Cluster start-up and admin operations
# Author:      Michael Kepple
# Called by:   ~/.bash_profile
# Date:        29 Feb 2014
# Note:        Needs updating.
#
HOSTNAME='echo $HOSTNAME | sed 's/\..*//''
SCRIPT_CONF_DIR=/home/hadoop/MastersProject/Machines/
NODES=('aho' 'tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
CLASS_ACER=('nano' 'ammo' 'spino' 'techno' 'dryo' 'grypo' 'seismo' 'anon');
LOW_END=('aho' 'rhino');
MID_END=('tito' 'maino' 'drapo' 'hippo' 'kepo');
HIGH_END=('newo' 'appo' 'mino');
sshAgentInfo=$HOME/.ssh/agentInfo

# Ex: ./clusterAdmin.sh -h NODES:BASIC
#     ./clusterAdmin.sh -h CLASS_ACER:BASIC
conf_sync()
{
    input=(${1//:/ })
    nodeClass=${input[0]}
    confClass=${input[1]}
    confDir=$SCRIPT_CONF_DIR$confClass/
    CLASS_NAME="${nodeClass}[@]"
    CLASS_ARRAY=( "${!CLASS_NAME}" );
    for node in ${CLASS_ARRAY[@]}
    do
        if [ "$node" = "$HOSTNAME" ]; then
            continue
        fi
	echo $node
        rsync -avz $confDir $node:/etc/hadoop/conf
    done
}

# ./clusterAdmin.sh -e "hostname; ls"
execute_nodes()
{
    for node in ${NODES[@]}
    do
        # force pseudo-tty allocation (allows for sudo, etc).
        ssh $node -t $OPTARG
    done
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
	#sshpass -p $1 scp /home/hadoop/clusterAdmin.sh $node:/home/hadoop/clusterAdmin.sh
        sshpass -p $1 ssh $node -t "chown hadoop:hadoop /etc/hadoop/conf/*"
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

# Argument is root pw on nodes, run as root
reboot()
{
    for node in ${NODES[@]}
    do
        sshpass -p $1 ssh $node -t "service hadoop-yarn-nodemanager restart"
	sshpass -p $1 ssh $node -t "service hadoop-hdfs-datanode restart"
    done
}

while getopts "h:s:i:e:r:" opt; do
    case $opt in
	e) execute_nodes $OPTARG
	   ;;
        h) conf_sync $OPTARG
           ;;
        i) init_passphrases $OPTARG
           ;;
        s) admin_sync $OPTARG
	   ;;
	r) reboot $OPTARG
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
