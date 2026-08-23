import json

from SL_Logger import streamLog, log, debug, warn, error

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

streamLog(console = True)

def delivery_report(err, msg):
    if err is not None:
        warn(f"Delivery failed for record {msg.key}: {err}")
    else:
        log(f"Record successfully produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

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

with open("paymentCreatedSchema.avsc", "r") as f:
    pcr_schema_str = f.read()

pcr_avro_serializer = AvroSerializer(schema_registry_client, pcr_schema_str)

with open("paymentCompletedSchema.avsc", "r") as f:
    pco_schema_str = f.read()

pco_avro_serializer = AvroSerializer(schema_registry_client, pco_schema_str)


# Sample payment creation
payment_creation_event = {
    "payment_id": "pay_1001",
    "merchant_id": "merch_abc",
    "amount": 49.99,
    "currency": "GBP",
    "timestamp": 1718376000000
}

# Sample payment completion
payment_completion_event = {
    "payment_id": "pay_1001",
    "merchant_id": "merch_abc",
    "status": "completed",
    "timestamp": 1718376000200
}

pcr_topic = "payment_created"

pco_topic = "payment_completed"

log("Creating Producer Item...")

log("Completing Producer...")

producer.produce(
    topic = pcr_topic,
    key = payment_creation_event["merchant_id"], # Here we partition by merchant_id
    value = pcr_avro_serializer(payment_creation_event, SerializationContext(pcr_topic, MessageField.VALUE)),
    on_delivery = delivery_report
)

log("Creating payment status...")

producer.produce(
    topic = pco_topic,
    key = payment_completion_event["merchant_id"],
    value = pco_avro_serializer(payment_completion_event, SerializationContext(pco_topic, MessageField.VALUE)),
    on_delivery = delivery_report
)

log("Created payment status...")

producer.flush()