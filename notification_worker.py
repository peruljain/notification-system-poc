#!/usr/bin/env python
import pika
import sys

# Get the routing key from the command-line arguments
if len(sys.argv) < 2:
    print("Usage: python script.py <routing_key>")
    sys.exit(1)

routing_key = sys.argv[1]

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='topic', exchange_type='topic')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='topic', queue=queue_name, routing_key=routing_key)

print(f' [*] Waiting for events with routing key "{routing_key}". To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(f" [x] {body}")

channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
