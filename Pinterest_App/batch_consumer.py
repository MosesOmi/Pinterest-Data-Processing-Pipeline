from pydoc_data.topics import topics
from kafka import KafkaConsumer
from json import loads
import json
import boto3
import os
import time

client = boto3.client('s3')

data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest"
)

data_stream_consumer.subscribe(topics=["Pinterestdata"])

path1 = os.path.join('/Users/mosesomiteru/Documents/AiCore_Stuff/Pinterest_App', 'data')
os.mkdir(path1)

for message in data_stream_consumer:
    print(message.value)
    with open(f'/Users/mosesomiteru/Documents/AiCore_Stuff/Pinterest_App/data/{message.offset}.json', 'w') as fp:
        json.dump(message.value, fp)
    response = client.upload_file(f'/Users/mosesomiteru/Documents/AiCore_Stuff/Pinterest_App/data/{message.offset}.json', 'pinterest-data-d408f649-ee2f-464f-8572-0fba5a860d8f', f'{message.offset}.json')
