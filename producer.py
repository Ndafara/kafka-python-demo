from random import randint

from kafka import KafkaProducer

RAW_TOPIC_NAME = 'raw_input'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

for _ in range(10):
    value = randint(10000, 100000)
    producer.send(RAW_TOPIC_NAME, bytes(str(value), encoding='utf-8'))

producer.flush()
print(f"Sent 10 random numbers as bytes to the kafka topic: {RAW_TOPIC_NAME}.")
