// order-init.js
// Initialises order_db, the database owned by the Order Service.
// Safe to run more than once.

const database = db.getSiblingDB("order_db");

// Create each collection only if it doesn't exist yet
const collections = ["orders", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// Fast lookup of a customer's orders
database.orders.createIndex({ customerId: 1 });

// Fast lookup of orders by state (CREATED, PREPARING, ...)
database.orders.createIndex({ status: 1 });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("order_db initialised: " + database.getCollectionNames().join(", "));