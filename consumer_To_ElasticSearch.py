
# coding: utf-8

# # consumer_To_ElasticSearch

# Este Script lee de kafka del topic "tagged_words" y las indexa en ElasticSearh

# In[1]:


# Imports
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import json


# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# INDEX_NAME = "quijote"
# if es.indices.exists(INDEX_NAME):
#     print("deleting '%s' index..." % (INDEX_NAME))
#     res = es.indices.delete(index = INDEX_NAME)
#     print(" response: '%s'" % (res))
# request_body = {
#     "settings" : {
#         "number_of_shards": 1,
#         "number_of_replicas": 0
#     }
# }
# print("creating '%s' index..." % (INDEX_NAME))
# res = es.indices.create(index = INDEX_NAME, body = request_body)
# print(" response: '%s'" % (res))

# In[ ]:


from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('tagged_words',bootstrap_servers=['localhost:9092'])
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


for message in consumer:
    data = message.value
    file_param = message.key
    print(json.loads(data))
    print(file_param)
    es.index(index=file_param, doc_type='strings', body=data)


# #delete test data and try with something more interesting
# es.indices.delete(index='palabras', ignore=[400, 404])
# 
# 

# es.index(index='palabras', doc_type='strings', body={
#                         "name": palabra,
#                         "linea":countLinea,
#                         "longitud":len(palabra),
#                         "index_start":index_ini,
#                         "index_end":index_end,
#                         "tag":  etiqueta,
#                     })
