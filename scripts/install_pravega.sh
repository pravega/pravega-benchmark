
. ./scripts/nodes.sh
scp cache/pravega-controller-0.1.0-SNAPSHOT.tar root@$CONTROLLER_NODE:/root/
ssh root@$CONTROLLER_NODE tar xvf ./pravega-controller-0.1.0-SNAPSHOT.tar
for SSS in $SSS_NODES
do
	scp cache/pravega-segmentstore-0.1.0-SNAPSHOT.tar root@$SSS:/root/
	ssh root@$SSS tar xvf ./pravega-segmentstore-0.1.0-SNAPSHOT.tar
	scp config/config.properties root@$SSS:/root/pravega-segmentstore-0.1.0-SNAPSHOT/conf/
done
