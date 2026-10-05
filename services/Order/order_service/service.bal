import ballerina/http;
import ballerina/log;
import ballerina/uuid;

function jsonResponse(int statusCode, json body) returns http:Response {
    http:Response res = new;
    res.statusCode = statusCode;
    res.setJsonPayload(body);
    return res;
}

function errorResponse(error e) returns http:Response {
    if e is OrderNotFoundError {
        return jsonResponse(http:STATUS_NOT_FOUND, {message: e.message()});
    }
    if e is InvalidTransitionError || e is ConcurrentUpdateError {
        return jsonResponse(http:STATUS_CONFLICT, {message: e.message()});
    }
    log:printError("Request failed", e);
    return jsonResponse(http:STATUS_INTERNAL_SERVER_ERROR, {message: "Internal server error"});
}

// Shared by cancel / confirm / prepare / ready
function changeStatusResponse(string orderId, string newStatus, string? reason = ()) returns http:Response {
    error? result = updateOrderStatus(orderId, newStatus, reason, "api");
    if result is error {
        return errorResponse(result);
    }
    Order|error o = fetchOrder(orderId);
    if o is error {
        return errorResponse(o);
    }
    return jsonResponse(http:STATUS_OK, o.toJson());
}

service /orders on new http:Listener(port) {

    // createOrder: save as CREATED, publish orders.created.
    // Optional "Idempotency-Key" header: a retry with the same key returns the original order.
    resource function post create(http:Request req) returns http:Response {
        json|error body = req.getJsonPayload();
        if body is error {
            return jsonResponse(http:STATUS_BAD_REQUEST, {message: "Request body must be valid JSON"});
        }
        CreateOrderRequest|error request = body.cloneWithType();
        if request is error {
            return jsonResponse(http:STATUS_BAD_REQUEST,
                {message: "Expected customerId, restaurantId and items (itemId, quantity, price)"});
        }
        if request.items.length() == 0 {
            return jsonResponse(http:STATUS_BAD_REQUEST, {message: "An order needs at least one item"});
        }
        decimal total = 0;
        foreach OrderItem item in request.items {
            if item.quantity <= 0 || item.price < 0d {
                return jsonResponse(http:STATUS_BAD_REQUEST, {message: "Invalid item quantity or price"});
            }
            total += item.price * <decimal>item.quantity;
        }

        string|http:HeaderNotFoundError idemKey = req.getHeader("Idempotency-Key");
        if idemKey is string {
            Order|error? prior = findOrderByIdempotencyKey(idemKey);
            if prior is error {
                return errorResponse(prior);
            }
            if prior is Order {
                return jsonResponse(http:STATUS_OK, prior.toJson());
            }
        }

        string now = nowIso();
        Order newOrder = {
            orderId: uuid:createType4AsString(),
            customerId: request.customerId,
            restaurantId: request.restaurantId,
            items: request.items,
            totalAmount: total,
            status: CREATED,
            createdAt: now,
            updatedAt: now
        };
        if idemKey is string {
            newOrder.idempotencyKey = idemKey;
        }

        error? saved = insertOrder(newOrder);
        if saved is error {
            return errorResponse(saved);
        }
        publishOrderCreated(newOrder);
        return jsonResponse(http:STATUS_CREATED, newOrder.toJson());
    }

    // getOrder
    resource function get [string orderId]() returns http:Response {
        Order|error o = fetchOrder(orderId);
        if o is error {
            return errorResponse(o);
        }
        return jsonResponse(http:STATUS_OK, o.toJson());
    }

    // audit trail of every status change
    resource function get [string orderId]/history() returns http:Response {
        Order|error o = fetchOrder(orderId);
        if o is error {
            return errorResponse(o);
        }
        StatusHistoryEntry[]|error history = listStatusHistory(orderId);
        if history is error {
            return errorResponse(history);
        }
        return jsonResponse(http:STATUS_OK, history.toJson());
    }

    // listOrdersByCustomer
    resource function get customer/[string customerId]() returns http:Response {
        Order[]|error orders = findOrdersByCustomer(customerId);
        if orders is error {
            return errorResponse(orders);
        }
        return jsonResponse(http:STATUS_OK, orders.toJson());
    }

    // listOrdersByRestaurant
    resource function get restaurant/[string restaurantId]() returns http:Response {
        Order[]|error orders = findOrdersByRestaurant(restaurantId);
        if orders is error {
            return errorResponse(orders);
        }
        return jsonResponse(http:STATUS_OK, orders.toJson());
    }

    // cancelOrder: only allowed in early states (CREATED, CONFIRMED)
    resource function post [string orderId]/cancel() returns http:Response {
        return changeStatusResponse(orderId, CANCELLED, "cancelled by user");
    }

    // confirmOrder
    resource function post [string orderId]/confirm() returns http:Response {
        return changeStatusResponse(orderId, CONFIRMED);
    }

    // startPreparing
    resource function post [string orderId]/prepare() returns http:Response {
        return changeStatusResponse(orderId, PREPARING);
    }

    // markReady
    resource function post [string orderId]/ready() returns http:Response {
        return changeStatusResponse(orderId, READY);
    }

    // healthCheck
    resource function get health() returns http:Response {
        if !checkDbConnection() {
            return jsonResponse(http:STATUS_SERVICE_UNAVAILABLE, {status: "DOWN", 'service: "order-service"});
        }
        return jsonResponse(http:STATUS_OK, {status: "UP", 'service: "order-service"});
    }
}
