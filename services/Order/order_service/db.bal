// order_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, indexes (customerId, restaurantId, status)

// insertOrder / findOrderById / findOrdersByCustomer / findOrdersByRestaurant
//     

// updateOrderStatusDoc(orderId, newStatus)
//     persist status change

// appendStatusHistory(entry)
//     audit trail of every transition

// findOrderByIdempotencyKey(key)
//     avoid duplicate orders on retry

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `order_db`. Never read another service's DB.
