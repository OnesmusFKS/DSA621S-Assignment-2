// notification_service: HTTP endpoints and business logic
// STUB FILE: comments only, no code. Implement each item below.

// buildMessage(eventType, payload) returns string
//     template per event and recipient type

// notifyCustomer / notifyRestaurant / notifyDriver
//     decide which recipients get which event

// sendEmail / sendSMS / sendPush
//     simulated channels (log/print + save)

// saveNotification(Notification)
//     

// GET /notifications/{recipientType}/{id} -> getNotifications
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

// channel send fails
//     retry, then log failure; must never block other consumers

// unknown event type
//     log and skip, do not crash the consumer

