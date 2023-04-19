import requests
import json_stream.requests
from kafka import KafkaProducer
from json_stream.dump import JSONStreamEncoder, default
import pickle
import json


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         x.encode("utf-8"),
                         key_serializer=lambda x:
                         x.encode("utf-8"),)

url = 'http://128.199.176.197:7551/streaming'
headers = {"Authorization":'Basic YTU3ZGUwODAtZjdiYy00MDIyLTkzZGMtNjEyZDJhZjU4ZDMxOg==', 'Connection':'keep-alive','Transfer-Encoding' : 'chunked'}

with requests.get(url, stream=True, headers=headers) as response:
    data = json_stream.requests.load(response, persistent=True)
    for obj in data:
        if obj.get('crawler_target') is not None:
            key = obj['crawler_target']['specific_resource_type']
            if (key in ['instagram', 'facebook', 'youtube', 'twitter']):
                asJSON = json.dumps(obj, default=default)
                producer.send('mytopic', value=asJSON, key=key)
