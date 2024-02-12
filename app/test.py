import json
from kafka import KafkaConsumer

# Define Kafka topic and consumer group ID
kafka_topic = 'mozio'

# Kafka broker configuration
bootstrap_servers = ['kafka:9092']

# Create Kafka consumer instance

consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
)
print('beggin')
print('aqui'*10)
for msg in consumer:
    print(f'Consumer {consumer}: {msg.value}')

print('aqui2'*10)

# Start consuming messages

print('end')
# Close the consumer when done
consumer.close()
