// payment_service: Kafka producers and consumers
// STUB FILE: comments only, no code. Implement each item below.

// initConsumer() returns error?
//     subscribe to orders.created and orders.cancelled

// onOrderCreated(OrderCreatedEvent)
//     check duplicate, call processPayment, publish result

// onOrderCancelled(event)
//     trigger refund if already paid

// publishPaymentCompleted(Payment)
//     producer: payments.completed

// publishPaymentFailed(Payment)
//     producer: payments.failed

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
