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
. ./scripts/nodes.sh
scp cache/pravega-controller-0.1.0-SNAPSHOT.tar root@$CONTROLLER_NODE:/root/
ssh root@$CONTROLLER_NODE tar xvf ./pravega-controller-0.1.0-SNAPSHOT.tar
for SSS in $SSS_NODES
do
	scp cache/pravega-segmentstore-0.1.0-SNAPSHOT.tar root@$SSS:/root/
	ssh root@$SSS tar xvf ./pravega-segmentstore-0.1.0-SNAPSHOT.tar
	scp config/config.properties root@$SSS:/root/pravega-segmentstore-0.1.0-SNAPSHOT/conf/
done
