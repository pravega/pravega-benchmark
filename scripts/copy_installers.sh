. ./scripts/nodes.sh
set -x
for ho in $NODES
do
  scp -r cache/*.* root@$ho:/root/
  scp  ./build/distributions/*.tar root@$ho:/root/
done
