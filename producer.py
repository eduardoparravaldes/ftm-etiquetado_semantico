
# coding: utf-8

# # PRODUCER KAFKA

# Paso 1, Ejecutamos el script producer. 
# Que lo que hace es leer un fichero (quijote1.txt) linea a linea y separ las lineas en palabras e insertar cada palabra en un topic del gestor de colas Kafka

# In[1]:


from kafka import KafkaProducer
import json


# In[2]:


#producer = KafkaProducer(bootstrap_servers='localhost:9092')
#producer.send ('palabras',json.dumps('test message').encode('utf-8'))


# In[6]:



producer = KafkaProducer(bootstrap_servers='localhost:9092')


'''
Leeremos un archivo de texto i lo insertamos en un topic de kafka
'''

# Abrir archivo para lectura
nombrearchivo = 'quijote1.txt'
with open(nombrearchivo) as archivo:
    contador = {} # diccionario para guardar ocurrencia de palabras
    grupo = {} # diccionario para agrupar palabras según su longitud
    for linea in archivo:
        palabras = linea.split() # separar línea en palabras, por espacio en blanco
        for palabra in palabras:
            palabra = palabra.lower().strip(".,") # cambiar a minúscula y quitar puntuación
            producer.send ('palabras', palabra )
               

# Cerrar archivo al finalizar
archivo.close()
# Mostrar información
print('Terminado...:')
    


# In[83]:


producer.flush()

