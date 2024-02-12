import json, asyncio
import logging
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from kafka import KafkaProducer, KafkaConsumer


app = FastAPI()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# Kafka settings
kafka_bootstrap_servers = ['kafka:9092']
kafka_topic = 'mozio'
kafka_group_id = 'tracking'


def get_kafka_producer():
    return KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)


@app.post("/produce/{message}")
async def produce_message(message: str):
    producer = get_kafka_producer()
    message = {'message': message}
    producer.send(kafka_topic, json.dumps(message).encode())
    producer.flush()
    return JSONResponse(content={"message": "Message produced successfully"})


def consume_messages(consumer):
    for msg in consumer:
        filename = f"iteration_{msg.value}_{msg.partition}_{id(consumer)}.txt"
        with open(filename, "w") as file:
            # The reason i am writing this file is so i have a log more reliable
            # i used prints in the past but running in the background it was not reliable at all
            file.write(f"Event message {msg.value} {msg.partition} {id(consumer)}\n")
        logging.info(f"Created file: {filename}")
        logging.info(f'Consumer {consumer}: {msg.value}')


def start_kafka_consumer():
    consumer = KafkaConsumer(
        kafka_topic,
        group_id=kafka_group_id,
        bootstrap_servers=kafka_bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    return consumer


@app.get("/consume")
async def consume_message(background_tasks: BackgroundTasks):
    consumer1: KafkaConsumer = start_kafka_consumer()
    # consumer2: KafkaConsumer = start_kafka_consumer()
    background_tasks.add_task(consume_messages, consumer1)

    # background_tasks.add_task(consume_messages, consumer2)
    return {"status": "Consumer started"}
