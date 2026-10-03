// payment_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, unique index on orderId

// insertPayment / findPaymentById / findPaymentByOrder / updatePaymentStatus
//     

// paymentExistsForOrder(orderId) returns boolean
//     idempotency check

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `payment_db`. Never read another service's DB.
