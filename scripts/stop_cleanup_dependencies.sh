set -x
. ./scripts/nodes.sh
    ssh root@$NAME_NODE /root/hadoop-2.7.3/bin/hdfs dfs -rm -r -f /_system
    ssh root@$NAME_NODE /root/hadoop-2.7.3/bin/hdfs dfs -rm -r -f /Scope
for NODE in $NODES
do
    ssh root@$NODE "lsof -i :3181 | grep -v PID | cut -d' ' -f 5 | xargs kill -9"
    ssh root@$NODE rm -rf /bk/index/* /bk/journal/* /bk/ledgers/*
    ssh root@$NODE /root/hadoop-2.7.3/sbin/stop-dfs.sh
    ssh root@$NODE "rm -rf /tmp/hadoop-root/*"
done
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkCli.sh deleteall /pravega
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh stop
