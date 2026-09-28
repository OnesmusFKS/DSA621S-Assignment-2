// order_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// isValidTransition(from, to) returns boolean
//     state machine guard (helper, not an endpoint)

// updateOrderStatus(orderId, newStatus, actor)
//     the ONLY function allowed to change status; validates, persists, logs history, publishes event

// POST /orders -> createOrder
//     validate, save as CREATED, publish orders.created

// GET /orders/{id} -> getOrder
//     

// GET /orders?customerId= / ?restaurantId= -> listOrders
//     

// GET /orders/{id}/history -> getStatusHistory
//     

// PUT /orders/{id}/preparing -> startPreparing
//     restaurant action

// PUT /orders/{id}/ready -> markReady
//     restaurant action

// PUT /orders/{id}/cancel -> cancelOrder
//     only allowed in early states

// GET /health -> healthCheck
//     

// ---- Cross-cutting ----

// validateRequest(input)
//     reusable input validation, return 400 with a clear message

// errorResponse(status, message)
//     consistent error format across all endpoints

// logEvent(level, message, context)
//     structured logging with orderId for tracing across services


// ---- Failure paths to handle ----

// payment failed -> CANCELLED
//     publish orders.cancelled so stock is released

// stock rejected / restaurant closed -> CANCELLED
//     notify customer

// payment or stock timeout
//     scheduled check: cancel orders stuck in CREATED beyond N minutes

// duplicate createOrder (client retry)
//     use idempotency key

// illegal transition attempt
//     return 409 Conflict, do not change state

