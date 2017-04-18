set -x
set -e
. ./scripts/nodes.sh
ssh root@$ZK_NODE tar xvzf ./zookeeper-3.5.1-alpha.tar.gz
ssh root@$ZK_NODE cp ./zookeeper-3.5.1-alpha/conf/zoo_sample.cfg ./zookeeper-3.5.1-alpha/conf/zoo.cfg
