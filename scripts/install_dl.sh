. ./scripts/nodes.sh
for NODE in $NODES
do
ssh root@$NODE unzip ./distributedlog-service-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip -d /opt/dl_all
scp ./scripts/entry_point.sh root@$NODE:/root/
done
