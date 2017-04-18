for ho in $NODES
do
  scp -r cache/*.* root@$ho:/root/
done
