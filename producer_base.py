from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka import Producer
from SL_Logger import log, warn, error

class StreamLedgerProducer:
    def __init__(self, bootstrap_servers = "localhost:9092", schema_registry_url = "http://localhost:8081"):
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    def delivery_report(self, err, msg):
        if err is not None:
            warn(f"Delivery failed for record {msg.key()}: {err}")
        else:
            log(f"Produced to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")

    def send_event(self, topic: str, schema_filename: str, event_data: dict, key_field: str):
        with open(schema_filename, "r") as file:
            schema_str = file.read()

        serializer = AvroSerializer(self.schema_registry_client, schema_str)

        partition_key = event_data[key_field]

        self.producer.produce(
            topic = topic,
            key = partition_key,
            value = serializer(event_data, SerializationContext(topic, MessageField.VALUE)),
            on_delivery = self.delivery_report
        )

        self.producer.flush()