<!-- - A Production-Grade Event-Driven Data Platform -->


<!-- 

    Docker Compose: Kafka + Zookeeper + Schema Registry + Kafka UI + MinIO
    Understand what each service does before moving on (read, don't just copy-paste)


    1. Write docker-compose.yml
    2. docker compose up -d
    3. Check Kafka UI — is the broker healthy?
    4. Create your topic (UI or CLI)
    5. Run your Python producer
    6. Watch messages arrive in Kafka UI
    7. Run your Python consumer


    ==============================================
    DOCKER COMPOSE MODEL
    ==============================================
        docker-compose.yml
        │
        ├── service: zookeeper
        │     └── runs on port 2181
        │
        ├── service: kafka
        │     ├── depends on: zookeeper
        │     └── runs on port 9092 (not 1234 — change your script)
        │
        ├── service: schema-registry
        │     ├── depends on: kafka
        │     └── runs on port 8081
        │
        └── service: kafka-ui
            ├── depends on: kafka
            └── runs on port 8080  ← open this in your browser

-->

### Why partition by merchant_id?

* Ordering Guarantees: Kafka guarantees strict chronological order only within a single partition. By using merchant_id, all events belonging to merch_abc land in the exact same partition. This means a refund or completion event will never get processed before its corresponding payment creation event for that specific merchant.

* Preventing Race Conditions: If you partitioned randomly (e.g., by payment_id), related events could scatter across different partitions and race each other, creating messy state logic downstream.

* Scalability: It distributes traffic evenly across merchants while grouping a single merchant's history together.
