set -x
. ./scripts/nodes.sh
for NODE in $NAME_NODE $DATA_NODES
do
	ssh root@$NODE "tar xvzf ./hadoop-2.7.3.tar.gz"
	scp ./config/*xml root@$NODE:/root/hadoop-2.7.3/etc/hadoop/
	ssh root@$NODE "sed -i 's/NAME/$NAME_NODE/g' /root/hadoop-2.7.3/etc/hadoop/core-site.xml"
done
#ssh root@$NAME_NODE "/root/hadoop-2.7.3/bin/hdfs namenode -format"
