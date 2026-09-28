// delivery_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// findAvailableDriver() / assignDriver(orderId)
//     selection logic (nearest or first available)

// handleNoDriverAvailable(orderId)
//     retry with backoff or leave pending; consider a timer/scheduled retry

// POST /drivers -> registerDriver
//     

// PUT /drivers/{id}/status -> updateDriverStatus
//     

// PUT /drivers/{id}/location -> updateDriverLocation
//     bonus: location simulation

// GET /deliveries/{id} -> getDelivery
//     

// GET /deliveries/order/{orderId} -> trackDelivery
//     

// PUT /deliveries/{id}/pickup -> confirmPickup
//     

// PUT /deliveries/{id}/complete -> completeDelivery
//     free the driver, publish delivery.completed

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

// no driver available
//     handleNoDriverAvailable: retry timer, don't lose the order

// driver goes offline mid-delivery
//     reassign or mark FAILED

// two orders claim the same driver
//     claimDriverAtomic

