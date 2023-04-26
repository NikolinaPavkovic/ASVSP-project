# load data to raw zone
#sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/producers/batch/batch.py #&

# transform data and load to transformation zone
#sudo docker exec -it spark-master ./spark/bin/spark-submit --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/transform.py #&

# save queries to curated zone
#sudo docker exec -it spark-master ./spark/bin/spark-submit --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/batch_queries.py

#sudo docker exec -it spark-master ./spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/consumer.py &

sudo docker exec -it spark-master ./spark/bin/spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 --jars ../spark/consumers/postgresql-42.5.1.jar ../spark/consumers/consumer_with_join.py
