#!/bin/bash
#
# Hadoop Cluster start-up and admin operations
# Author:      Michael Kepple
# Called by:   ~/.bash_profile
# Date:        29 Feb 2014
#
HOSTNAME='echo $HOSTNAME | sed 's/\..*//''
SCRIPT_CONF_DIR=/home/hadoop/MastersProject/Machines/
DFS_DIRS=('/home/hdfs/' '/tmp/hdfs');
GATEWAY=aho
NODES=('tonto' 'aho' 'tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
SLAVES=('tonto' 'tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
CLASS_ACER=('nano' 'ammo' 'spino' 'techno' 'dryo' 'grypo' 'seismo' 'anono');
LOW_END=('aho' 'rhino');
MID_END=('tito' 'maino' 'drapo' 'hippo' 'kepo');
MID_HIGH=('mino' 'tonto');
APPO=('appo');
NEWO=('newo');
LARGE_HEAP=('newo' 'appo' 'tonto' 'mino');
sshAgentInfo=$HOME/.ssh/agentInfo

# Run to incorporate new Datanode/Nodemanager slaves into cluster
# NOTE: run as root, argument is main node.
# ./clusterAdmin.sh -n aho
install_node()
{
    scp $1:/home/hadoop/.ssh/authorized_keys /home/hadoop/.ssh/authorized_keys
    scp $1:/etc/hosts /etc/hosts
    scp $1:/home/hadoop/.bash_profile /home/hadoop/.bash_profile
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
    yum remove hadoop-hdfs-journalnode
    rm -rf /etc/hadoop/conf
    scp -r $1:/etc/hadoop/conf /etc/hadoop/conf
    mkdir -p /home/hdfs/dfs/data
    chown -R hdfs /home/hdfs
    mkdir -p /home/hdfs/yarn
    chown -R yarn /home/hdfs/yarn
    mkdir -p /tmp/hdfs/dfs/data
    chown -R hdfs /tmp/hdfs
    yum install rsync
    yum install dmidecode
    yum install hdparm
    scp -r $1:/etc/hadoop/conf /etc/hadoop/
    mkdir -p /home/hadoop-yarn/cache/hadoop/nm-local-dir
    chmod -R yarn /home/hadoop-yarn
    service hadoop-hdfs-datanode start
    service hadoop-yarn-nodemanager start
}

# Ex: sudo ./clusterAdmin.sh -d
reformat_datanodes()
{
    stty -echo
    read -p "Admin Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
    	for dir in ${DFS_DIRS[@]}
    	do
		sshpass -p $passw ssh root@$node -t "rm -f $dir/dfs/data/current/VERSION"
	done
    done
}

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

# Update conf files for LARGE_HEAP cluster nodes
# Ex: sudo ./clusterAdmin.sh -l
large_heap_sync()
{
    conf_sync NEWO:LARGE_HEAP_MACHINES/NEWO
    conf_sync APPO:LARGE_HEAP_MACHINES/APPO
    conf_sync MID_HIGH:LARGE_HEAP_MACHINES/MID_HIGH
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${LARGE_HEAP[@]}
    do
        sshpass -p $passw ssh root@$node -t "service hadoop-yarn-nodemanager restart"
        sshpass -p $passw ssh root@$node -t "service hadoop-hdfs-datanode restart"
    done
    sshpass -p $passw ssh root@aho -t "service hadoop-yarn-resourcemanager restart"
}

# Ex: ./clusterAdmin.sh -a
conf_sync_all()
{
    conf_sync CLASS_ACER:CLASS_ACER
    conf_sync LOW_END:LOW_END
    conf_sync MID_END:MID_END
    conf_sync MID_HIGH:MID_HIGH
    conf_sync APPO:APPO
    conf_sync NEWO:NEWO
}

# sudo ./clusterAdmin.sh -c
clear_logs()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw ssh $node -t "rm -rf /var/log/hadoop-hdfs/*"
        sshpass -p $passw ssh $node -t "rm -rf /var/log/hadoop-mapreduce/*"
        sshpass -p $passw ssh $node -t "rm -rf /var/log/hadoop-yarn/*"
        sshpass -p $passw ssh $node -t "rm -rf /var/log/hadoop-httpfs/*"
    done
}

# ./clusterAdmin.sh -e "hostname; ls"
execute_nodes()
{
    for node in ${SLAVES[@]}
    do
        # force pseudo-tty allocation (allows for sudo, etc).
        ssh $node -t $OPTARG
    done
}

# Note: master node must have installed sshpass
admin_sync()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw scp /etc/hosts root@$node:/etc/hosts
        sshpass -p $passw scp /home/hadoop/.ssh/authorized_keys root@$node:/home/hadoop/.ssh/authorized_keys
	sshpass -p $passw ssh root@$node -t chown hadoop /home/hadoop/.ssh/authorized_keys
	sshpass -p $passw scp /home/hadoop/.bash_profile root@$node:/home/hadoop/.bash_profile
	sshpass -p $passw scp /home/hadoop/.bashrc root@$node:/home/hadoop/.bashrc
	sshpass -p $passw scp /home/hadoop/clusterAdmin.sh root@$node:/home/hadoop/clusterAdmin.sh
        sshpass -p $passw ssh root@$node -t "chown hadoop:hadoop /etc/hadoop/conf/*"
	#sshpass -p $passw scp /etc/sysctl.conf $node:/etc/sysctl.conf
	#sshpass -p $passw scp /etc/security/limits.conf $node:/etc/security/limits.conf
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

# sudo ./clusterAdmin.sh -r
reboot()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw ssh root@$node -t "service hadoop-yarn-nodemanager restart"
	sshpass -p $passw ssh root@$node -t "service hadoop-hdfs-datanode restart"
        sshpass -p $passw ssh root@$node -t "service gmond restart"
    done
    sshpass -p $passw ssh root@aho -t "service hadoop-yarn-resourcemanager restart"
}

# ./clusterAdmin.sh -f
# NOTE: should be run on gateway node as su/sudo
gateway_forward()
{
    iptables -A FORWARD -i eth1 -j ACCEPT
    iptables -A FORWARD -o eth0 -j ACCEPT
    iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
}

# sudo ./clusterAdmin.sh -s
solo_mode()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw ssh root@$node -t "service hadoop-yarn-nodemanager stop"
    done
    sudo -u hadoop conf_sync NEWO:NEWO_SOLO
    service hadoop-yarn-nodemanager restart
    sshpass -p $passw ssh root@aho -t "service hadoop-yarn-resourcemanager restart"
}

# sudo ./clusterAdmin.sh -u
# NOTE: must be run as root
undo_solo()
{
    conf_sync NEWO:NEWO
    reboot
}

while getopts "h:si:e:rn:adicoul" opt; do
    case $opt in
	e) execute_nodes $OPTARG
	   ;;
        h) conf_sync $OPTARG
           ;;
        i) init_passphrases $OPTARG
           ;;
        s) admin_sync
	   ;;
	r) reboot
	   ;;
	n) install_node $OPTARG
	   ;;
        a) conf_sync_all
           ;;
	d) reformat_datanodes
	   ;;
        c) clear_logs
           ;;
	o) solo_mode
           ;;
        u) undo_solo
           ;;
        l) large_heap_sync
           ;;
    esac
done

# On gateway node, make sure iptables is properly configured.
if [ "$HOSTNAME" == "$GATEWAY" ]
then
    gateway_forward
fi

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
