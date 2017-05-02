set -e
set -x
wget -c http://www.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz -o ./cache/hadoop-2.7.3.tar.gz
wget -c http://www.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz  -o ./cache/bookkeeper-server-4.4.0-bin.tar.gz
wget -c http://www.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz -o ./cache/zookeeper-3.5.1-alpha.tar.gz

