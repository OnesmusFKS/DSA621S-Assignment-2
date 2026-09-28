// customer_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// POST /customers -> registerCustomer
//     validate input, reject duplicate email

// GET /customers/{id} -> getCustomer
//     

// PUT /customers/{id} -> updateCustomer
//     

// DELETE /customers/{id} -> deleteCustomer
//     

// POST /customers/{id}/addresses -> addAddress
//     

// GET /customers/{id}/addresses -> getAddresses
//     

// PUT /customers/{id}/addresses/{addrId} -> updateAddress
//     

// DELETE /customers/{id}/addresses/{addrId} -> removeAddress
//     

// GET /customers/{id}/orders -> getOrderHistory
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

