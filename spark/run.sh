# load data to raw zone
sudo docker exec -it spark-master ./spark/bin/spark-submit  ../spark/producers/batch/batch.py

# transform data and load to transformation zone