from pydoc_data.topics import topics
from kafka import KafkaConsumer
from json import loads

data_stream_consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",    
    value_deserializer=lambda message: loads(message),
    auto_offset_reset="earliest" 
)

data_stream_consumer.subscribe(topics=["Pinterestdata"])

for message in data_stream_consumer:
    print(message.value)