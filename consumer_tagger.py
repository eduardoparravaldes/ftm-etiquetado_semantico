
# coding: utf-8

# # consumer_tagger
# 
# Lee de kafka etiqueta y devuelve a kafka

# 1. Cargar los ficheros de diccionario de palabras
# 2. Para leer los mensajes nos suscribimos a el topic de Kafka, etiquetamos y enviamos a kafka nuevamente los mensajes etiquetados.
# 
# NOTA:  
# Primero Ejecutar el consumer, asi, nos suscribirnos al topic de la cola de kafka y asi estamos esperando a leer los mensajes. Y ya luego el producer.   

# In[1]:


# Imports
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json


# In[ ]:


def load_simple_words():
    # cargamos el fichero de palabras y etiquetas desde parquet
    df = spark.read.load("simple_words.parquet")
    # Registrar el DataFrame como tabla.
    df.createOrReplaceTempView("simple_words")
    df.cache()
    print ("load simple_words OK")
    return df
        


# df = spark.sql('SELECT *  FROM simple_words' )
# df is None

# In[ ]:


topic_consumer='files_to_tag'
topic_producer='tagged_words'

consumer = KafkaConsumer(topic_consumer,bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


#Cargamos diccionario con tags
load_simple_words()

for message in consumer:
    data = message.value
    file_param = message.key
    print (file_param)
    #print(data)
    countLinea = int(1)
    for linea in data.split("\n"):
            #print ("linea {0} : countLinea: {1} ".format(countLinea, linea))
            palabras = linea.split() # separar línea en palabras, por espacio en blanco
            start = int(0)
            for palabra in palabras:
                palabra = palabra.lower().strip(")(.,\n")
                #print palabra
                index_ini = linea.lower().index(palabra, start)
                index_end = int(index_ini+len(unicode(palabra, encoding='utf-8')))
                start = index_end
                #print (unicode(palabra, encoding='utf-8'), " datos: ", countLinea, index_ini ,index_end )
                simple_word_query =""
                simple_word_query = spark.sql('SELECT *  FROM simple_words WHERE lower(TOKEN) =="{}"'.format(palabra))
                tag =""
                if simple_word_query.count()>0: 
                    simple_word_query.show()
                    tag = simple_word_query.collect()[0].SEMTAG
                    #print (unicode(palabra, encoding='utf-8'), " datos: ", countLinea, index_ini ,index_end, etiqueta)                   
                    datos = {
                        'file':file_param,
                        'palabra': palabra,
                        'linea': countLinea,
                        'longitud': len(palabra),
                        'index_start':index_ini,
                        'index_end':index_end,
                        'tag':tag
                    }
                producer.send(topic_producer, key=file_param, value=datos) 
                json_str = json.dumps(datos)
                print('Datos en formato JSON:', json_str)
            countLinea = countLinea+1


# call(["/opt/Kafka/kafka_2.11-1.0.0/bin/kafka-topics", "--zookeeper", "zookeeper-1:2181", "--delete", "--topic", topic_name])
