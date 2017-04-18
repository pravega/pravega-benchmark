set -x
. ./scripts/nodes.sh
for NODE in $NODES
do
    ssh root@$NODE "ps -ef |grep -i distributedlog | grep -v grep | cut -d' ' -f 2,7 | xargs kill -9"
    ssh root@$NODE rm -rf /bk/*
done
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkCli.sh deleteall /pravega
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh stop
