from kafka import KafkaConsumer
import time
import json
from notification_publisher import MessageSender

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'notification_topic',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',  # Start reading at the earliest message
    group_id='notification'
)

notification_publisher = MessageSender()

# Consume messages
for message in consumer:
    try:
        # Deserialize the JSON message
        json_message = json.loads(message.value.decode("utf-8"))
        print(f'Received message: {json_message}')
        routing_key = json_message["userType"]
        notification_publisher.send_message(json_message, routing_key)
    except json.JSONDecodeError as e:
        print(f'Error decoding JSON: {e}')
    
    time.sleep(1)

# Close the consumer
consumer.close()
