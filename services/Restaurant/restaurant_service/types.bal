// restaurant_service: Records / enums to define
// STUB FILE: comments only, no code. Implement each item below.

// NOTE: Ballerina packages can't share code: duplicate any shared records per service and keep them matching docs/events.md.

// record Restaurant
//     id, name, cuisine, address, phone, isActive

// record MenuItem
//     id, restaurantId, name, description, price, category, available

// record InventoryItem
//     menuItemId, restaurantId, quantity, lowStockThreshold

// record OpeningHours
//     restaurantId, per-day open/close times, holiday overrides

// record OrderCreatedEvent
//     payload consumed from orders.created

// record StockResultEvent
//     orderId, restaurantId, reserved (bool), reason
