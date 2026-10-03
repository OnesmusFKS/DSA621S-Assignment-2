// delivery_service: Records / enums to define
// STUB FILE: comments only, no code. Implement each item below.

// NOTE: Ballerina packages can't share code: duplicate any shared records per service and keep them matching docs/events.md.

// enum DriverStatus
//     AVAILABLE, BUSY, OFFLINE

// enum DeliveryStatus
//     PENDING_ASSIGNMENT, ASSIGNED, PICKED_UP, IN_TRANSIT, DELIVERED, FAILED

// record Driver
//     id, name, phone, vehicle, status, currentLocation

// record Delivery
//     id, orderId, driverId, restaurantAddress, customerAddress, status, timestamps

// record Location
//     lat, lng, updatedAt

// record DeliveryEvent
//     payload published

// record OrderReadyEvent
//     payload consumed
