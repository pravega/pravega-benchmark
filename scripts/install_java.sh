. ./scripts/nodes.sh
for ho in $NODES
do
  ssh root@$ho zypper --non-interactive install  jdk* 
done
