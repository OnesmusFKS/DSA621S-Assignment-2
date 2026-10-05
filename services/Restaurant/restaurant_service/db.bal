// restaurant_service: Database access functions
// STUB FILE: comments only, no code. Implement each item below.

// initDb() returns error?
//     create client, indexes

// insertRestaurant / findRestaurant / findAllRestaurants / updateRestaurantDoc
//     restaurant CRUD

// insertMenuItem / findMenuByRestaurant / updateMenuItemDoc / deleteMenuItemDoc
//     menu CRUD

// saveOpeningHours / findOpeningHours
//     hours storage

// decrementStockAtomic(menuItemId, qty) returns boolean
//     atomic conditional decrement so two orders can't oversell

// incrementStock(menuItemId, qty)
//     release stock on cancel/failure

// ---- Reliability (all services) ----

// insertProcessedEvent / findProcessedEvent
//     processed_events collection, unique index on eventId

// checkDbConnection() returns boolean
//     used by /health

// Database isolation: this service owns ONLY `restaurant_db`. Never read another service's DB.
