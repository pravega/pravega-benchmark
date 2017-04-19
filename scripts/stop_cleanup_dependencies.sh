set -x
. ./scripts/nodes.sh
for NODE in $NODES
do
    ssh root@$NODE "lsof -i :3181 | grep -v PID | cut -d' ' -f 5 | xargs kill -9"
    ssh root@$NODE rm -rf /bk/*
    ssh root@$NODE /root/hadoop-2.7.3/sbin/stop-dfs.sh
done
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkCli.sh deleteall /pravega
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh stop
