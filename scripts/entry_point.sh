#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

PORT0=${PORT0:-$bookiePort}
PORT0=${PORT0:-3181}
ZK_URL=${ZK_URL:-127.0.0.1:2181}
USE_MOUNT=${USE_MOUNT:-0}
PRAVEGA_PATH=${PRAVEGA_PATH:-"pravega"}
BK_CLUSTER_NAME=${BK_CLUSTER_NAME:-"bookkeeper"}

BK_LEDGERS_PATH="/${PRAVEGA_PATH}/${BK_CLUSTER_NAME}/ledgers"

if [ $USE_MOUNT -eq 0 ]; then
    BK_DIR="/bk"
else
    BK_DIR=$MESOS_SANDBOX
fi

echo "bookie service port0 is $PORT0 "
echo "ZK_URL is $ZK_URL"
echo "BK_DIR is $BK_DIR"
echo "BK_LEDGERS_PATH is $BK_LEDGERS_PATH"

sed -i 's/3181/'$PORT0'/' ./bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i "s/localhost:2181/${ZK_URL}/" ./bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|journalDirectory=/tmp/bk-txn|journalDirectory='${BK_DIR}'/journal|' ./bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|ledgerDirectories=/tmp/bk-data|ledgerDirectories='${BK_DIR}'/ledgers|' ./bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|indexDirectories=/tmp/data/bk/ledgers|indexDirectories='${BK_DIR}'/index|' ./bookkeeper-server-4.4.0/conf/bk_server.conf
sed -i 's|# zkLedgersRootPath=/ledgers|zkLedgersRootPath='${BK_LEDGERS_PATH}'|' ./bookkeeper-server-4.4.0/conf/bk_server.conf


echo "create the zk root"
./zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}
./zookeeper-3.5.1-alpha/bin/zkCli.sh -server $ZK_URL create /${PRAVEGA_PATH}/${BK_CLUSTER_NAME}

echo "format the bookie"
# format bookie
BOOKIE_CONF=${PWD}/bookkeeper-server-4.4.0/conf/bk_server.conf ./bookkeeper-server-4.4.0/bin/bookkeeper shell metaformat -nonInteractive

echo "start a new bookie"
# start bookie,
SERVICE_PORT=$PORT0 nohup ${PWD}/bookkeeper-server-4.4.0//bin/bookkeeper bookie --conf  ${PWD}/bookkeeper-server-4.4.0/conf/bk_server.conf 0<&- &> /tmp/nohup.log &
sleep 5

