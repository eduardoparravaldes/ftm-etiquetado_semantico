
# coding: utf-8

# # PRODUCER KAFKA

# Script Producer con Funciones para leer ficheros y enviar a kafka

# In[85]:


from kafka import KafkaProducer
import json


# In[92]:


def upload_full_file(file,topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    '''
    Lee un archivo y lo envia a un topic de kafka
    '''
    try:
        # Abrir archivo para lectura
        with open(file, "r") as file_object:
            data = file_object.read()
            producer.send (topic, data )
            #print (data)   
        # Cerrar archivo al finalizar
        file_object.close()
        # Mostrar información
        print('Enviado OK!')  
    except IOError as e:
        print "I/O error({0}): {1} : {2}".format(e.errno, e.strerror, file)
    except: #handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]


# In[93]:


def upload_file_by_lines(file,topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    '''
    Lee un archivo y lo envia a un topic de kafka
    '''
    try:
        # Abrir archivo para lectura
        countLinea = int(1)
        with open(file, "r") as file_object:
            data = file_object.read()
            for linea in data.split("\n"):
                producer.send (topic, linea)
                countLinea = countLinea +1
                #print (data)   
        # Cerrar archivo al finalizar
        file_object.close()
        # Mostrar información
        print('Enviado OK!')  
    except IOError as e:
        print "I/O error({0}): {1} : {2}".format(e.errno, e.strerror, file)
    except: #handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]


# In[94]:


def upload_file_by_words(file,topic):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    '''
    Lee un archivo y lo envia a un topic de kafka
    '''
    try:
        # Abrir archivo para lectura
        with open(file, "r") as file_object:
            data = file_object.read()
            for linea in data.split("\n"):
                palabras = linea.split() # separar línea en palabras, por espacio en blanco
                for palabra in palabras:
                    palabra = palabra.lower().strip(".,") # cambiar a minúscula y quitar puntuación
                    producer.send (topic, palabra )
        # Cerrar archivo al finalizar
        file_object.close()
        # Mostrar información
        print('Enviado OK!')  
    except IOError as e:
        print "I/O error({0}): {1} : {2}".format(e.errno, e.strerror, file)
    except: #handle other exceptions such as attribute errors
        print "Unexpected error:", sys.exc_info()[0]


# Ejemplos de llamadas a las funciones

# In[95]:


#upload_full_file('quijote1.txt', "files")


# In[96]:


#upload_file_by_lines('quijote1.txt', "lines")


# In[97]:


#upload_file_by_lines('quijote1.txt', "words")

