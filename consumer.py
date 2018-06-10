
# coding: utf-8

# # Consumer

# Leemos del topic de Kafka e insertamos en ElasticSearch las palabras.  
# primero ejecutamos el consumer y luego el producer.    

# In[ ]:


#from kafka import KafkaConsumer
#consumer = KafkaConsumer('palabras',bootstrap_servers=['localhost:9092'])
#for msg in consumer:
     #print (msg)


# In[ ]:


from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('palabras',bootstrap_servers=['localhost:9092'])

#connect to our cluster
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


contador = {} # diccionario para guardar ocurrencia de palabras
grupo = {} # diccionario para agrupar palabras según su longitud
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%b" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
    palabra = message.value
    
    es.index(index='palabras', doc_type='strings', body={
        "name": palabra,
        "longitud":len(palabra),
        "tag": "palabra del libro quijote"
    })


        


# In[ ]:


#delete test data and try with something more interesting
#es.indices.delete(index='books', ignore=[400, 404])

