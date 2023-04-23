# chmod +x delete.sh

#delete folders created previously
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /raw"