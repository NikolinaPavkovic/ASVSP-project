from kafka import KafkaProducer
import kafka.errors
import csv
import time
import operator


KAFKA_TOPIC = "flights-topic"
KAFKA_BROKER = "kafka2:19093"

print("Kafka producer app started...")
while True:
    try:
        kafka_producer = KafkaProducer(bootstrap_servers = KAFKA_BROKER,
                                    value_serializer=lambda x: x.encode('utf-8'))
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

with open("./datasets/realtime_final.csv") as csv_file:
    csv_reader = csv.reader(csv_file)
    header = next(csv_reader)
    print("Header", header)

    message = None
    for row in csv_reader:
        message_field_value_list = []
        for value in row:
            message_field_value_list.append(value)
            message = ",".join(message_field_value_list)
        print(row)
        kafka_producer.send(KAFKA_TOPIC, message)
        message_field_value_list = []
        message = None
        time.sleep(1)
kafka_producer.flush()









