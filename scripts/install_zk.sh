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
set -e
. ./scripts/nodes.sh
for NODE in $NODES
do
ssh root@$NODE tar xvzf ./zookeeper-3.5.1-alpha.tar.gz
ssh root@$NODE cp ./zookeeper-3.5.1-alpha/conf/zoo_sample.cfg ./zookeeper-3.5.1-alpha/conf/zoo.cfg
done
