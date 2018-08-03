
# coding: utf-8

# # PRODUCER KAFKA

# Script Producer lee ficheros y enviar a kafka

# In[1]:


from kafka import KafkaProducer
import json
producer = KafkaProducer(bootstrap_servers='localhost:9092')


# In[2]:


def upload_full_file(file_param,topic_param):
    '''
    Lee un archivo y lo envia a un topic de kafka
    '''
    try:
        # Abrir archivo para lectura
        with open(file_param, "r") as file_object:
            data = file_object.read() 
            print(file_param.split(".")[0])
            producer.send (topic_param, key=file_param, value=data )  
        # Cerrar archivo al finalizar
        file_object.close()
        # Mostrar información
        print('Enviado OK!')  
    except IOError as e:
        print "I/O error({0}): {1} : {2}".format(e.errno, e.strerror, file_param)
    except: #handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]


# Ejemplos de llamadas a las funciones

# In[3]:


upload_full_file('quijote_test.txt', 'files_to_tag')

