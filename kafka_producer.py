import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

productos = ["arroz", "leche", "pan", "huevos", "azucar"]

print("Enviando datos a Kafka...")

while True:
    data = {
        "producto": random.choice(productos),
        "precio": random.randint(1000, 10000),
        "cantidad": random.randint(1, 5)
    }

    producer.send("ventas_d1", value=data)
    print("Enviado:", data)

    time.sleep(2)
