""" 

Week 1 — Docker Stack & Kafka Basics

    - Docker Compose: Kafka + Zookeeper + Schema Registry + Kafka UI + MinIO
    - Understand what each service does before moving on (read, don't just copy-paste)
    - Create 2 Kafka topics: payments and refunds, partitioned by merchant_id
    - Send 10 manual test messages via Kafka UI to verify setup

    * Time budget: 6 - 8 hrs

Key concept: Why partition by merchant_id? Write 3 sentences answering this.

"""

from docker_pipeline import add_nums

def test_add_nums():
    assert add_nums(1, 2) == 3