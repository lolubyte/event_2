from kafka import KafkaConsumer,KafkaProducer
import json
from  elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk


es = Elasticsearch()

host = 'search-event-ra2qdnq2ogub6pxtxxnjpa5sue.us-east-1.es.amazonaws.com'
es = Elasticsearch(
    hosts=[{'host': host, 'port': 80}],
)
print(es.info())

#producer = KafkaProducer(bootstrap_servers='10.10.0.68:9092,10.10.2.138:9092,10.10.3.137:9092')
consumer = KafkaConsumer(bootstrap_servers='10.10.0.68:9092,10.10.2.138:9092,10.10.3.137:9092',auto_offset_reset='earliest',value_deserializer=lambda m: json.loads(m.decode('utf-8')))
consumer.subscribe(['demo'])
docs = []
count = 0
for msg in consumer:
    data = msg[6]
    i = 1
    res = es.index(index="shareprice",doc_type='price',id=i,body=data)
    i = i + 1







    #if(msg[6]['TICKER'] == 'AMZN'):
    #    print("putting into AMNZ topic",msg[6])
    #    producer.send('AMNZ',json.dumps(msg[6]))

