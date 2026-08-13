import json

from SL_Logger import streamLog, log, debug, warn, error

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

streamLog(console = True)

log("Starting StreamLedger Producer...")

schema_registry_config = { 
    "url": "http://localhost:8081" 
}
schema_registry_client = SchemaRegistryClient(schema_registry_config)

producer_config = {
    "bootstrap.servers": "localhost:9092"
}
producer = Producer(producer_config)

log("Opening schema...")

with open("schema.avsc", "r") as f:
    schema_str = f.read()

avro_serializer = AvroSerializer(schema_registry_client, schema_str)

def delivery_report(err, msg):
    if err is not None:
        warn(f"Delivery failed for record {msg.key}: {err}")
    else:
        log(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

# Sample payment
payment_event = {
    "payment_id": "pay_1001",
    "merchant_id": "merch_abc",
    "amount": 49.99,
    "currency": "GBP",
    "timestamp": 1718376000000
}

topic = "payment_created"

log("Creating Producer Item...")

log("Completing Producer...")

producer.produce(
    topic = topic,
    key = payment_event["merchant_id"], # Here we partition by merchant_id
    value = avro_serializer(payment_event, SerializationContext(topic, MessageField.VALUE)),
    on_delivery = delivery_report
)

producer.flush()