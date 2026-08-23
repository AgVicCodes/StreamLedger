## Commands
* Start Docker: docker compose up -d

### How to get everything at once
To make sure you don't run into any more missing dependency errors for Avro and Schema Registry, install the full schema suite package:
* pip install "confluent-kafka[avro,json,protobuf]"