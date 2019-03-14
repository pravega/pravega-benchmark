#!/usr/bin/env bash
#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#


set -x
. ./scripts/nodes.sh
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh start
for NODE in $NODES
do
    ssh root@$NODE ZK_URL=$ZK_NODE:2181 /root/entry_point.sh 
done

ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh start
ssh root@$NAME_NODE "echo 1 > /proc/sys/net/ipv6/conf/public/disable_ipv6"
ssh root@$NAME_NODE "/root/hadoop-2.7.3/bin/hdfs namenode -format"
ssh root@$NAME_NODE /root/hadoop-2.7.3/sbin/hadoop-daemon.sh start namenode

echo "Starting datanodes on $DATA_NODES"
for NODE in $DATA_NODES
do
	ssh root@$NODE "echo 1 > /proc/sys/net/ipv6/conf/public/disable_ipv6"
	ssh root@$NODE /root/hadoop-2.7.3/sbin/hadoop-daemon.sh start datanode
done
