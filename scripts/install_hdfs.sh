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
for NODE in $NAME_NODE $DATA_NODES
do
	ssh root@$NODE "tar xvzf ./hadoop-2.7.3.tar.gz"
	scp ./config/*xml root@$NODE:/root/hadoop-2.7.3/etc/hadoop/
	ssh root@$NODE "sed -i 's/NAME/$NAME_NODE/g' /root/hadoop-2.7.3/etc/hadoop/core-site.xml"
done
#ssh root@$NAME_NODE "/root/hadoop-2.7.3/bin/hdfs namenode -format"
