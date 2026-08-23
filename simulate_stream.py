import time
import random
import uuid
from SL_Logger import streamLog, log
from producer_base import StreamLedgerProducer

streamLog(console = True)
client = StreamLedgerProducer()

# Mock data pools for randomization
MERCHANTS = ["merch_abc", "merch_xyz", "merch_123", "merch_store_99"]
CURRENCIES = ["GBP", "EUR", "USD"]

log("Starting StreamLedger Event Simulation...")

for i in range(100): # Simulate 100 transaction cycles
    # Generate realistic unique IDs and data
    payment_id = f"pay_{uuid.uuid4().hex[:8]}"
    merchant_id = random.choice(MERCHANTS)
    amount = round(random.uniform(10.0, 1000.0), 2)
    currency = random.choice(CURRENCIES)

    # Standard current timestamp in millis
    current_time_ms = int(time.time() * 1000)

    # 1. Payment Created Event
    log("Creating Payment Event...")

    client.send_event(
        topic = "payment_created",
        schema_filename = "schema/paymentCreatedSchema.avsc",
        event_data = {
            "payment_id": payment_id,
            "merchant_id": merchant_id,
            "amount": amount,
            "currency": currency,
            "timestamp": current_time_ms,
        },
        key_field = "merchant_id"
    )

    # Intentional duplicate: re_sending the same payment_created event
    # (Simulating an upstream retry or network glitch)
    if i % 7 == 0:
        log(f"Injecting intentional duplicate for {payment_id}")
        client.send_event(
            topic = "payment_created",
            schema_filename = "schema/paymentCreatedSchema.avsc",
            event_data = {
                "payment_id": payment_id,
                "merchant_id": merchant_id,
                "amount": amount,
                "currency": currency,
                "timestamp": current_time_ms,
            },
            key_field = "merchant_id"
        )

    # INTENTIONAL LATE EVENT: Simulate an event arriving with a timestamp 2 hours in the past
    if i % 11 == 0 and i != 77:
        log(f"-> Injecting intentional late event for {payment_id}")
        late_timestamp = current_time_ms - (2 * 60 * 60 * 1000)
        client.send_event(
            topic = "payment_created",
            schema_filename = "schema/paymentCreatedSchema.avsc",
            event_data = {
                "payment_id": payment_id,
                "merchant_id": merchant_id,
                "amount": amount,
                "currency": currency,
                "timestamp": late_timestamp,
            },
            key_field = "merchant_id"
        )

    # 2. Payment Completed Event (Follows creation closely)
    client.send_event(
        topic = "payment_completed",
        schema_filename = "schema/paymentCompletedSchema.avsc",
        event_data = {
            "payment_id": payment_id,
            "merchant_id": merchant_id,
            "status": "completed",
            "timestamp": current_time_ms + 1500 # 1.5 seconds later
        },
        key_field = "merchant_id"
    )

    # Random chance to trigger a refund or chargeback workflow:
    if random.random() < 0.3: # 30% chance
        log(f"Triggering a refund/chargeback")
        client.send_event(
            topic = "refund_initiated",
            schema_filename = "schema/refundInitiatedSchema.avsc",
            event_data = {
                "payment_id": payment_id,
                "merchant_id": merchant_id,
                "amount": round(amount * 0.5, 2), # Partial refund
                "timestamp": current_time_ms + 5000 # 1.5 seconds later
            },
            key_field = "merchant_id"            
        )

    # Sleep tightly between iterations to mimic real traffic flow
    time.sleep(1)

log("Simulation batch complete!")