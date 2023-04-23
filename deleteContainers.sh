# delete all containers

cd hdfs
docker-compose down

cd ../kafka
docker-compose down

cd ../client
docker-compose down

cd ../spark
docker-compose down

#delete network

docker network rm globalNetwork