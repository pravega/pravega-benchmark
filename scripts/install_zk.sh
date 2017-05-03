set -x
set -e
. ./scripts/nodes.sh
for NODE in $NODES
do
ssh root@$NODE tar xvzf ./zookeeper-3.5.1-alpha.tar.gz
ssh root@$NODE cp ./zookeeper-3.5.1-alpha/conf/zoo_sample.cfg ./zookeeper-3.5.1-alpha/conf/zoo.cfg
done
