set -x
. ./scripts/nodes.sh
for NODE in $SSS_NODES
 do 
	ssh root@$NODE "ps -ef |grep controllerU |grep -v grep| awk \" { print \\\$2; }\" | xargs kill -9 "
done
	ssh root@$CONTROLLER_NODE "ps -ef |grep controller.conf |grep -v grep| awk \" { print \\\$2; }\" | xargs kill -9 "
