// delivery_service: Kafka producers and consumers
// STUB FILE: comments only, no code. Implement each item below.

// initConsumer() returns error?
//     subscribe to orders.status.changed (READY)

// onOrderReady(OrderReadyEvent)
//     create Delivery, call assignDriver

// publishDeliveryAssigned(Delivery)
//     producer: delivery.assigned

// publishDeliveryPickedUp(Delivery)
//     producer: delivery.pickedup

// publishDeliveryCompleted(Delivery)
//     producer: delivery.completed

// ---- Reliability (all services) ----

// isEventProcessed(eventId) returns boolean
//     idempotency check against a processed_events collection

// markEventProcessed(eventId)
//     record it AFTER successful handling (consider a TTL index)

// handleWithRetry(event, handler)
//     retry a failing handler N times with backoff before giving up

// publishToDlq(originalTopic, event, errorReason)
//     send to <topic>.dlq with error details

// shutdownKafka()
//     close producer/consumer cleanly on service stop

