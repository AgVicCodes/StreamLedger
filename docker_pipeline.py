""" 

Week 1 — Docker Stack & Kafka Basics

    - Docker Compose: Kafka + Zookeeper + Schema Registry + Kafka UI + MinIO
    - Understand what each service does before moving on (read, don't just copy-paste)
    - Create 2 Kafka topics: payments and refunds, partitioned by merchant_id
    - Send 10 manual test messages via Kafka UI to verify setup

    * Time budget: 6 - 8 hrs

Key concept: Why partition by merchant_id? Write 3 sentences answering this.

"""

import kafka
import pytest
import logging

from kafka import KafkaConsumer, KafkaProducer


def streamLog():
    logging.basicConfig(filename = "stream_log.log", level = 10)

def add_nums(a, b):
    return a + b

consumer = KafkaConsumer("my_favourite_topic", group_id = "my_favourite_group")
for msg in consumer:
    print(msg)

producer = KafkaProducer(bootstrap_servers = "localhost:1234")

for _ in range(100):
    producer.send("foobar", b"some_message_bytes")



future = producer.send("foobar", b"another_message")

result = future.get(timeout = 60)

producer.flush()