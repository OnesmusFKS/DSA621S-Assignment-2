// payment_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// processPayment(orderId, amount) returns Payment
//     simulate outcome (e.g. random or amount-based failure)

// refundPayment(orderId)
//     mark REFUNDED, publish refund event

// GET /payments/{id} -> getPayment
//     

// GET /payments/order/{orderId} -> getPaymentByOrder
//     

// POST /payments/{orderId}/refund -> manual refund
//     

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

// duplicate orders.created delivery
//     must NOT charge twice: check paymentExistsForOrder

// refund for order that was never paid
//     no-op with clear result

