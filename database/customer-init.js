// customer-init.js
// Initialises customer_db, the database owned by the Customer Service.
// Safe to run more than once.

const database = db.getSiblingDB("customer_db");

// Create each collection only if it doesn't exist yet
const collections = ["customers", "addresses", "order_history", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// No two customers can share an email
database.customers.createIndex({ email: 1 }, { unique: true });

// Fast lookup of a customer's addresses and past orders
database.addresses.createIndex({ customerId: 1 });
database.order_history.createIndex({ customerId: 1 });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("customer_db initialised: " + database.getCollectionNames().join(", "));