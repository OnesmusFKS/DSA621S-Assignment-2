// notification-init.js
// Initialises notification_db, the database owned by the Notification Service.
// Safe to run more than once.

const database = db.getSiblingDB("notification_db");

// Create each collection only if it doesn't exist yet
const collections = ["notifications", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// Fast lookup of all notifications for one customer, restaurant or driver
database.notifications.createIndex({ recipientId: 1 });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print(
  "notification_db initialised: " + database.getCollectionNames().join(", "),
);
