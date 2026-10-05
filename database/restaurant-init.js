// restaurant-init.js
// Initialises restaurant_db, the database owned by the Restaurant Service.
// Safe to run more than once.

const database = db.getSiblingDB("restaurant_db");

// Create each collection only if it doesn't exist yet
const collections = ["restaurants", "menu_items", "inventory", "opening_hours", "processed_events"];
const existing = database.getCollectionNames();
collections.forEach((name) => {
  if (!existing.includes(name)) {
    database.createCollection(name);
  }
});

// Fast lookup of a restaurant's menu
database.menu_items.createIndex({ restaurantId: 1 });

// One stock document per menu item
database.inventory.createIndex({ menuItemId: 1 }, { unique: true });

// One set of opening hours per restaurant
database.opening_hours.createIndex({ restaurantId: 1 }, { unique: true });

// Idempotency: an event can only be recorded once
database.processed_events.createIndex({ eventId: 1 }, { unique: true });

print("restaurant_db initialised: " + database.getCollectionNames().join(", "));