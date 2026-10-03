// delivery_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, indexes (driver status, orderId)

// insertDriver / findDriver / updateDriverStatusDoc / updateDriverLocation
//     

// findAvailableDrivers()
//     candidates for assignment

// claimDriverAtomic(driverId) returns boolean
//     AVAILABLE -> BUSY atomically to avoid double assignment

// insertDelivery / findDeliveryByOrder / updateDeliveryStatusDoc
//     

// findPendingDeliveries()
//     for retry of unassigned orders

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `delivery_db`. Never read another service's DB.
