import logging
import json
from kafka import KafkaProducer

# Enable logging
logging.basicConfig(level=logging.ERROR)

# Initialize the Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:29092')
topic = 'notification_topic'

for i in range(1, 100):
    # Define the topic and message
    message = None
    if i%2 == 0:
        message = {
            "userType": "VIP",
            "orderId": f"uuid-{i}"
        }
    elif i%3 == 0:
        message = {
            "userType": "PREMIUM",
            "orderId": f"uuid-{i}"
        }
    else:
        message = {
            "userType": "NORMAL",
            "orderId": f"uuid-{i}"
        }    

    partition_key = str(i)

    # Serialize the message to JSON
    json_message = json.dumps(message).encode('utf-8')

    # Send the message
    future = producer.send(topic, key=partition_key.encode('utf-8'), value=json_message)

    try:
        # Block until a single message is sent (or timeout)
        record_metadata = future.get(timeout=10)
    except Exception as e:
        logging.error('Error sending message', exc_info=e)
    else:
        logging.info(f'Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}')

# Ensure all messages are sent before closing the producer
producer.flush()

# Close the producer
producer.close()
