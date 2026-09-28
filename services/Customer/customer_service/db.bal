// customer_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, ensure collections/indexes (unique email)

// insertCustomer / findCustomerById / findCustomerByEmail / updateCustomerDoc / deleteCustomerDoc
//     customer CRUD

// insertAddress / findAddressesByCustomer / updateAddressDoc / deleteAddressDoc
//     address CRUD

// clearDefaultAddress(customerId)
//     ensure only one default address

// upsertOrderSummary(OrderSummary)
//     write/update history from events

// findOrdersByCustomer(customerId)
//     read history

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `customer_db`. Never read another service's DB.
