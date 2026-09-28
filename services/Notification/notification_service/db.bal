// notification_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, indexes (recipientId, orderId)

// insertNotification / findNotificationsByRecipient / findNotificationsByOrder
//     log of everything sent

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `notification_db`. Never read another service's DB.
