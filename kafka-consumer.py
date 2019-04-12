from kafka import KafkaConsumer,KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)


consumer = KafkaConsumer(bootstrap_servers="b-2.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092,b-3.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092,b-1.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092",auto_offset_reset='earliest',enable_auto_commit=True,value_deserializer=lambda m: json.loads(m.decode('utf-8')))

#subscribing the tooic

consumer.subscribe(['demo'])

for msg in consumer:
	if(msg[6]["PRICE"] >= 60):
		print("Consumed Records from demo",msg[6])
