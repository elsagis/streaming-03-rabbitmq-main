"""
    This program sends a message to a queue on the RabbitMQ server.

    Author: Denise Case
    Date: January 14, 2023
    
    Student/Editor Name: Elsa Ghirmazion
    Date 29/Jan/2023

"""

# add imports at the beginning of the file
import pika
#Declare a variable foe a message so it only has to be updated in one plase
Message = "...Lets wrap this up"
# create a blocking connection to the RabbitMQ server
conn = pika.BlockingConnection(pika.ConnectionParameters("LOCALHOST"))
# use the connection to create a communication channel
ch = conn.channel()
# use the channel to declare a queue
ch.queue_declare(queue="hello")
# use the channel to publish a message to the queue
ch.basic_publish(exchange="", routing_key="hello", body=Message)
# print a message to the console for the user
print(Message)
# close the connection to the server
conn.close()
