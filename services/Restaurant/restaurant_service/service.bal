// restaurant_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// POST /restaurants -> registerRestaurant
//

// GET /restaurants -> listRestaurants
//     optional filter: open now, cuisine

// GET /restaurants/{id} -> getRestaurant
//

// PUT /restaurants/{id} -> updateRestaurant
//

// POST /restaurants/{id}/menu -> addMenuItem
//

// GET /restaurants/{id}/menu -> getMenu
//

// PUT /restaurants/{id}/menu/{itemId} -> updateMenuItem
//

// DELETE /restaurants/{id}/menu/{itemId} -> removeMenuItem
//

// PUT /restaurants/{id}/hours -> setOpeningHours
//

// GET /restaurants/{id}/hours -> getOpeningHours
//

// GET /restaurants/{id}/open -> isOpenNow
//

// GET /restaurants/{id}/inventory -> checkStock
//

// PUT /restaurants/{id}/inventory/{itemId} -> updateStock
//     manual restock

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

// simultaneous orders for last item
//     use decrementStockAtomic, reject the loser

// partial stock (order has 3 items, 1 unavailable)
//     reject whole order and roll back reservations

// restaurant closed
//     reject with reason
