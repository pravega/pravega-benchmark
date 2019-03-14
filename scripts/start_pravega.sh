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
export PRAVEGA_SEGMENTSTORE_OPTS="-Dpravegaservice.zkURL=$ZK_NODE:2181  -Dbookkeeper.zkAddress=$ZK_NODE:2181  -Dhdfs.hdfsUrl=hdfs://$NAME_NODE:9000 -DautoScale.controllerUri=tcp://$CONTROLLER_NODE:9090  -Dpravegaservice.controllerUri=pravega://$CONTROLLER_NODE:9090"
export PRAVEGA_CONTROLLER_OPTS="-DZK_URL=$ZK_NODE:2181 -DCONTROLLER_SERVER_PORT=9090 -DREST_SERVER_PORT=9091 -Dlog.level=INFO"
echo "$SERVER_OPTS"
ssh root@$CONTROLLER_NODE "export PRAVEGA_CONTROLLER_OPTS=\"$PRAVEGA_CONTROLLER_OPTS\";./pravega-controller-0.1.0-SNAPSHOT/bin/pravega-controller 0<&- &> /tmp/controller.log &"

for NODE in $SSS_NODES
do
	echo $HOST_OPTS
	ssh root@$NODE "export PRAVEGA_SEGMENTSTORE_OPTS=\"$PRAVEGA_SEGMENTSTORE_OPTS\";./pravega-segmentstore-0.1.0-SNAPSHOT/bin/pravega-segmentstore 0<&- &> /tmp/host.log &"
done
 
