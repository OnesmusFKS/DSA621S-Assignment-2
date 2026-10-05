// admin-init.js
// Initialises admin_db, the database owned by the Admin Service.
// Safe to run more than once.

const database = db.getSiblingDB("admin_db");

// Create each collection only if it doesn't exist yet
const collections = ["restaurant_stats", "delivery_stats", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// One stats document per restaurant
database.restaurant_stats.createIndex({ restaurantId: 1 }, { unique: true });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("admin_db initialised: " + database.getCollectionNames().join(", "));
