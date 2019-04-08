import json
import boto3

import random
import datetime
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='10.10.0.68:9092,10.10.2.138:9092,10.10.3.137:9092')

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
        print(data)
        producer.send('demo',data)