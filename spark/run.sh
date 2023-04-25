# load data to raw zone
sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/producers/batch/batch.py &

# transform data and load to transformation zone
sudo docker exec -it spark-master ./spark/bin/spark-submit --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/transform.py &

# save queries to curated zone
sudo docker exec -it spark-master ./spark/bin/spark-submit --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/batch_queries.py