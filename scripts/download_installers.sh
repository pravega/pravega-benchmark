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

set -e
set -x
wget -c http://www.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz -o ./cache/hadoop-2.7.3.tar.gz
wget -c http://www.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz  -o ./cache/bookkeeper-server-4.4.0-bin.tar.gz
wget -c http://www.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz -o ./cache/zookeeper-3.5.1-alpha.tar.gz

