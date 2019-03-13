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
set -x
for ho in $NODES
do
  scp -r cache/*.* root@$ho:/root/
  scp  ./build/distributions/*.tar root@$ho:/root/
done
