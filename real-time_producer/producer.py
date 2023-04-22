from kafka import KafkaProducer
import kafka.errors
import csv
import time
import operator


KAFKA_TOPIC = "flights-topic"
KAFKA_BROKER = "kafka2:19093"

if __name__ == "__main__":

    print("Kafka producer app started ...")
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
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_reader = sorted(csv_reader, key=operator.itemgetter(0), reverse=False)

        line_count = 0
        message = None
        for row in csv_reader:
            message_field_value_list = []
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
            else:
                for value in row:
                    message_field_value_list.append(value)
                    message = ",".join(message_field_value_list)
                print(message)
                kafka_producer.send(KAFKA_TOPIC, message)

            line_count += 1    
            time.sleep(1)

        print(f'Processed {line_count} lines.')









