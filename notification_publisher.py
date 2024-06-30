import pika
import json

class MessageSender:
    def __init__(self, host='localhost'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='topic', exchange_type='topic')

    def send_message(self, message, routing_key):
        json_message = json.dumps(message)
        self.channel.basic_publish(exchange='topic', routing_key=routing_key, body=json_message)
        print(f" [x] Sent {message}")

    def close_connection(self):
        self.connection.close()