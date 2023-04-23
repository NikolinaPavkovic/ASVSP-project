# create network
docker network create --driver bridge globalNetwork

# run all containers

cd hdfs
docker-compose up -d

cd ../kafka
docker-compose up -d

cd ../client
docker-compose up -d

cd ../spark
docker-compose up -d
