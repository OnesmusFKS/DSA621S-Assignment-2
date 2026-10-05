// payment-init.js
// Initialises payment_db, the database owned by the Payment Service.
// Safe to run more than once.

const database = db.getSiblingDB("payment_db");

// Create each collection only if it doesn't exist yet
const collections = ["payments", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// One payment per order
database.payments.createIndex({ orderId: 1 }, { unique: true });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("payment_db initialised: " + database.getCollectionNames().join(", "));