# Architecture (STUB)

TODO: diagram (services, Kafka, per-service DBs), and the event flow for a full order:
CREATED -> stock + payment -> CONFIRMED -> PREPARING -> READY -> driver assigned -> OUT_FOR_DELIVERY -> DELIVERED

Also: failure flow (payment failed, out of stock, no driver).
