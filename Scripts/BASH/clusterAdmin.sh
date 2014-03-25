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
NODES=('aho' 'tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
SLAVES=('tito' 'spino' 'nano' 'ammo' 'techno' 'dryo' 'grypo' 'anono' 'seismo' 'rhino' 'maino' 'newo' 'appo' 'drapo' 'mino' 'hippo' 'kepo');
CLASS_ACER=('nano' 'ammo' 'spino' 'techno' 'dryo' 'grypo' 'seismo' 'anono');
LOW_END=('aho' 'rhino');
MID_END=('tito' 'maino' 'drapo' 'hippo' 'kepo');
MINO=('mino');
APPO=('appo');
NEWO=('newo');
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
    service hadoop-hdfs-datanode start
    service hadoop-yarn-nodemanager start
}

# Ex: sudo ./clusterAdmin.sh -d
# NOTE: Must be run as su/sudo
reformat_datanodes()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
    	for dir in ${DFS_DIRS[@]}
    	do
		sshpass -p $passw ssh $node -t "rm -f $dir/dfs/data/current/VERSION"
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

# Ex: ./clusterAdmin.sh -a
conf_sync_all()
{
    conf_sync CLASS_ACER:CLASS_ACER
    conf_sync LOW_END:LOW_END
    conf_sync MID_END:MID_END
    conf_sync MINO:MINO
    conf_sync APPO:APPO
    conf_sync NEWO:NEWO
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

# Should be run as root/sudo'd
# Note: master node must have installed sshpass
admin_sync()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw scp /etc/hosts $node:/etc/hosts
        sshpass -p $passw scp /home/hadoop/.ssh/authorized_keys $node:/home/hadoop/.ssh/authorized_keys
	sshpass -p $passw ssh $node -t chown hadoop /home/hadoop/.ssh/authorized_keys
	sshpass -p $passw scp /home/hadoop/.bash_profile $node:/home/hadoop/.bash_profile
	sshpass -p $passw scp /home/hadoop/.bashrc $node:/home/hadoop/.bashrc
	sshpass -p $passw scp /home/hadoop/clusterAdmin.sh $node:/home/hadoop/clusterAdmin.sh
        sshpass -p $passw ssh $node -t "chown hadoop:hadoop /etc/hadoop/conf/*"
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
# NOTE: must be run as su/sudo
reboot()
{
    stty -echo
    read -p "Password: " passw; echo
    stty echo
    for node in ${NODES[@]}
    do
        sshpass -p $passw ssh $node -t "service hadoop-yarn-nodemanager restart"
	sshpass -p $passw ssh $node -t "service hadoop-hdfs-datanode restart"
    done
}

# ./clusterAdmin.sh -f
# NOTE: should be run on gateway node as su/sudo
gateway_forward()
{
    iptables -A FORWARD -i eth1 -j ACCEPT
    iptables -A FORWARD -o eth0 -j ACCEPT
    iptables -t nat -A POSTROUTING -o eth0 -j MASQUERADE
}

while getopts "h:si:e:rn:ad" opt; do
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
