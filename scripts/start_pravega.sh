

set -x
. ./scripts/nodes.sh
export HOST_OPTS="-Dpravegaservice.zkURL=$ZK_NODE:2181  -Dbookkeeper.zkAddress=$ZK_NODE:2181  -Dhdfs.hdfsUrl=hdfs://$NAME_NODE:9000 -DautoScale.controllerUri=tcp://$CONTROLLER_NODE:9090  -Dpravegaservice.controllerUri=pravega://$CONTROLLER_NODE:9090"
export SERVER_OPTS="-DZK_URL=$ZK_NODE:2181 -DCONTROLLER_SERVER_PORT=9090 -DREST_SERVER_PORT=9091 -Dlog.level=INFO"
echo "$SERVER_OPTS"
ssh root@$CONTROLLER_NODE "export SERVER_OPTS=\"$SERVER_OPTS\";./pravega-controller-0.1.0-SNAPSHOT/bin/pravega-controller 0<&- &> /tmp/controller.log &"

for NODE in $SSS_NODES
do
	echo $HOST_OPTS
	ssh root@$NODE "export HOST_OPTS=\"$HOST_OPTS\";./pravega-segmentstore-0.1.0/bin/pravega-segmentstore 0<&- &> /tmp/host.log &"
done
 
