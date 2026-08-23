from SL_Logger import streamLog, log
from producer_base import StreamLedgerProducer

streamLog(console = True)
client = StreamLedgerProducer()

# Payment Created Producer
client.send_event(
    topic = "payment_created",
    schema_filename = "schema/paymentCreatedSchema.avsc",
    event_data = {
        "payment_id": "pay_1001",
        "merchant_id": "merch_abc", 
        "amount": 49.99,
        "currency": "GBP",
        "timestamp": 1718376000000
    },
    key_field = "merchant_id"
)

# Payment Completed Producer
client.send_event(
    topic = "payment_completed",
    schema_filename = "schema/paymentCompletedSchema.avsc",
    event_data = {
        "payment_id": "pay_1001",
        "merchant_id": "merch_abc",
        "status": "completed",
        "timestamp": 1718376000200
    },
    key_field = "merchant_id"
)

# Refund Initiated Producer
client.send_event(
    topic = "refund_initiated",
    schema_filename = "schema/refundInitiatedSchema.avsc",
    event_data = {
        "payment_id": "pay_1001",
        "merchant_id": "merch_abc",
        "amount": 49.99,
        "timestamp": 1718376000600
    },
    key_field = "merchant_id"
)

# Refund Completed Producer
client.send_event(
    topic = "refund_completed",
    schema_filename = "schema/refundCompletedSchema.avsc",
    event_data = {
        "payment_id": "pay_1001",
        "merchant_id": "merch_abc",
        "status": "completed",
        "timestamp": 1718376001000
    },
    key_field = "merchant_id"
)

# Chargeback Opened Producer
client.send_event(
    topic = "chargeback_opened",
    schema_filename = "schema/chargebackOpenedSchema.avsc",
    event_data = {
        "payment_id": "pay_1001",
        "merchant_id": "merch_abc",
        "percentage": 0.05,
        "timestamp": 1718376001400
    },
    key_field = "merchant_id"
)