// order_service: Kafka producers and consumers
// STUB FILE: comments only, no code. Implement each item below.

// initConsumers() returns error?
//     subscribe to payments.completed, payments.failed, stock results, delivery.assigned, delivery.completed

// publishOrderCreated(Order)
//     producer: orders.created (key = orderId)

// publishStatusChanged(Order)
//     producer: orders.status.changed

// publishOrderCancelled(Order)
//     producer: orders.cancelled

// onPaymentCompleted(event)
//     CREATED -> CONFIRMED

// onPaymentFailed(event)
//     -> CANCELLED

// onStockRejected(event)
//     -> CANCELLED

// onDeliveryAssigned(event)
//     record driver on the order

// onDeliveryPickedUp(event)
//     READY -> OUT_FOR_DELIVERY

// onDeliveryCompleted(event)
//     -> DELIVERED

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

