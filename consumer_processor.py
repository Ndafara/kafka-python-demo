from kafka import KafkaConsumer
from kafka import KafkaProducer

CONSUME_TOPIC_NAME = 'raw_input'
PROCESSED_TOPIC_NAME = 'processed'

KAFKA_SERVER = 'localhost:9092'

consumer = KafkaConsumer(CONSUME_TOPIC_NAME)
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)


def odd_or_even(byte_input):
	random_number = int(byte_input.decode("utf-8"))
	print(random_number)

	if (random_number % 2) == 0:
		return str(random_number)+'-E'
	else:
		return str(random_number) + '-O'


for message in consumer:
	print(message)
	result = odd_or_even(message.value)
	print(result)
	producer.send(PROCESSED_TOPIC_NAME, bytes(str(result), encoding='utf-8'))
	producer.flush()
