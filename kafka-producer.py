import json
import random
import datetime
import logging


from kafka import KafkaProducer
logging.basicConfig(level=logging.INFO)


producer = KafkaProducer(bootstrap_servers="b-2.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092,b-1.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092,b-3.da70lchko2b25vtopj9u6nh61.c1.kafka.us-east-1.amazonaws.com:9092",key_serializer=str.encode,acks='all')

def getReferrer():
    data = {}
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['EVENT_TIME'] = str_now
    data['TICKER'] = random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV'])
    price = random.random() * 100
    data['PRICE'] = round(price, 2)
    return data

while True:
        data = json.dumps(getReferrer())
        logging.info("Received Sample Data")
        try:
            response = producer.send('demo',key='keyused',value=data)
            logging.info("Sending Records to Kafka Topic demo")
        except Exception as e:
            logging.error("Exception occurred", exc_info=True)
