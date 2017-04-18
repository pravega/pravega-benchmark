set -x
set -e
. ./scripts/nodes.sh
ssh root@$ZK_NODE ./zookeeper-3.5.1-alpha/bin/zkServer.sh start
for NODE in $NODES
do
    ssh root@$NODE ZK_URL=$ZK_NODE:2181 /root/entry_point.sh 
done
