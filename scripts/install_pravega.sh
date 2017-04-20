
. ./scripts/nodes.sh
scp cache/server.tar root@$CONTROLLER_NODE:/root/
ssh root@$CONTROLLER_NODE tar xvf ./server.tar
for SSS in $SSS_NODES
do
	scp cache/host.tar root@$SSS:/root/
	scp config/config.properties root@$SSS:/root/
	ssh root@$SSS tar xvf ./host.tar
done
