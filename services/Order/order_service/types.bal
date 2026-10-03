// order_service: Records / enums to define
// STUB FILE: comments only, no code. Implement each item below.

// NOTE: Ballerina packages can't share code: duplicate any shared records per service and keep them matching docs/events.md.

// enum OrderStatus
//     CREATED, CONFIRMED, PREPARING, READY, OUT_FOR_DELIVERY, DELIVERED, CANCELLED

// record Order
//     id, customerId, restaurantId, items, total, deliveryAddress, status, createdAt, updatedAt

// record OrderItem
//     menuItemId, name, quantity, unitPrice

// record OrderInput
//     request body for createOrder

// record StatusHistoryEntry
//     orderId, from, to, timestamp, actor

// record OrderEvent
//     payload published to order topics

// record PaymentEvent / DeliveryEvent / StockResultEvent
//     payloads consumed
