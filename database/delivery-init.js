// delivery-init.js
// Initialises delivery_db, the database owned by the Delivery Service.
// Safe to run more than once.

const database = db.getSiblingDB("delivery_db");

// Create each collection only if it doesn't exist yet
const collections = ["drivers", "deliveries", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// One delivery per order
database.deliveries.createIndex({ orderId: 1 }, { unique: true });

// Fast lookup of available drivers
database.drivers.createIndex({ available: 1 });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("delivery_db initialised: " + database.getCollectionNames().join(", "));
