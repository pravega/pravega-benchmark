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
for NODE in $SSS_NODES
 do 
	ssh root@$NODE "ps -ef |grep controllerU |grep -v grep| awk \" { print \\\$2; }\" | xargs kill -9 "
done
	ssh root@$CONTROLLER_NODE "ps -ef |grep controller.conf |grep -v grep| awk \" { print \\\$2; }\" | xargs kill -9 "
