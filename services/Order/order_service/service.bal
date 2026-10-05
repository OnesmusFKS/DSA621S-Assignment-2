import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int orderServicePort = ?;
configurable string mongoDatabase = "order_db";

const string ORDERS_COLLECTION = "orders";
const string TOPIC_ORDER_CREATED = "orders.created";
const string TOPIC_ORDER_STATUS_CHANGED = "orders.status.changed";
const decimal POLL_TIMEOUT_SECONDS = 1;

// Types 
type OrderItem record {|
    string itemId;
    string name?;
    int quantity;
    decimal price;
|};

type CreateOrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
|};

type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    string createdAt;
    string updatedAt;
|};

type OrderNotFoundError distinct error;
type InvalidTransitionError distinct error;

// Statuses 
const string CREATED = "CREATED";
const string CONFIRMED = "CONFIRMED";
const string PREPARING = "PREPARING";
const string READY = "READY";
const string OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY";
const string DELIVERED = "DELIVERED";
const string CANCELLED = "CANCELLED";

final readonly & map<string[]> allowedTransitions = {
    [CREATED]: [CONFIRMED, CANCELLED],
    [CONFIRMED]: [PREPARING, CANCELLED],
    [PREPARING]: [READY],
    [READY]: [OUT_FOR_DELIVERY],
    [OUT_FOR_DELIVERY]: [DELIVERED],
    [DELIVERED]: [],
    [CANCELLED]: []
};

//  Clients 
final kafka:Producer orderEventProducer = check new (kafkaBroker, acks = kafka:ACKS_ALL);
final kafka:Consumer paymentConsumer = check new (kafkaBroker, groupId = "order-service-payment");
final kafka:Consumer deliveryConsumer = check new (kafkaBroker, groupId = "order-service-delivery");
final mongodb:Client dbClient = check new ({connection: mongoUri});

// Subscribe and start the consumer loops on their own strands so the HTTP listener can start too.
function init() returns error? {
    check paymentConsumer->subscribe(["payments.completed", "payments.failed"]);
    check deliveryConsumer->subscribe(["delivery.assigned", "delivery.picked_up", "delivery.delivered"]);
    _ = start pollLoop(paymentConsumer);
    _ = start pollLoop(deliveryConsumer);
}

// Kafka client setup 
function getKafkaProducer() returns kafka:Producer {
    return orderEventProducer;
}

// Payment consumer
function getKafkaConsumer() returns kafka:Consumer {
    return paymentConsumer;
}

function getDeliveryConsumer() returns kafka:Consumer {
    return deliveryConsumer;
}

function getDbClient() returns mongodb:Client {
    return dbClient;
}

function getOrdersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check dbClient->getDatabase(mongoDatabase);
    return db->getCollection(ORDERS_COLLECTION);
}

// Persistence helpers 
function fetchOrder(string orderId) returns Order|error {
    mongodb:Collection collection = check getOrdersCollection();
    Order?|error found = collection->findOne({orderId}, targetType = Order);
    if found is error {
        return found;
    }
    if found is () {
        return error OrderNotFoundError(string `Order ${orderId} not found`);
    }
    return found;
}

function listOrders(map<json> filter) returns Order[]|error {
    mongodb:Collection collection = check getOrdersCollection();
    stream<Order, error?> results = check collection->find(filter, targetType = Order);
    return from Order o in results
        select o;
}
function nowString() returns string {
    return time:utcToString(time:utcNow());
}

// State machine 
function isValidTransition(string currentStatus, string newStatus) returns boolean {
    string[]? allowed = allowedTransitions[currentStatus];
    if allowed is () {
        return false;
    }
    return allowed.indexOf(newStatus) is int;
}

// The only function that changes status.
function updateOrderStatus(string orderId, string newStatus, string? reason = ()) returns error? {
    Order existing = check fetchOrder(orderId);

    // Idempotent: events can be delivered more than once.
    if existing.status == newStatus {
        return;
    }
    if !isValidTransition(existing.status, newStatus) {
        return error InvalidTransitionError(
            string `Cannot move order ${orderId} from ${existing.status} to ${newStatus}`);
    }

    mongodb:Collection collection = check getOrdersCollection();
    string updatedAt = nowString();
    // Filtering on the current status guards against concurrent updates.
    mongodb:UpdateResult result = check collection->updateOne(
        {orderId, status: existing.status},
        {set: {status: newStatus, updatedAt}}
    );
    if result.matchedCount == 0 {
        return error InvalidTransitionError(
            string `Order ${orderId} was changed by another request, please retry`);
    }

    Order updated = existing.clone();
   updated.status = newStatus;
   updated.updatedAt = updatedAt;
    publishStatusChanged(updated, reason);
}

// Kafka producers
function publishEvent(string topic, string key, json payload) {
    kafka:Error? sent = orderEventProducer->send({
        topic,
        key: key.toBytes(),
        value: payload.toJsonString().toBytes()
    });
    if sent is kafka:Error {
        log:printError("Failed to publish event", sent, topic = topic, key = key);
    }
}

// Publishes to orders.status.changed. Confirmed and cancelled orders are also published to
// their own topics, which the notification service listens to.
function publishStatusChanged(Order o, string? reason = ()) {
    json event = {
        orderId: o.orderId,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        status: o.status,
        reason: reason,
        updatedAt: o.updatedAt
    };
    publishEvent(TOPIC_ORDER_STATUS_CHANGED, o.orderId, event);

    if o.status == CONFIRMED {
        publishEvent("orders.confirmed", o.orderId, event);
    } else if o.status == CANCELLED {
        publishEvent("orders.cancelled", o.orderId, event);
    }
}

// Kafka consumers 
function pollLoop(kafka:Consumer consumer) {
    while true {
        kafka:BytesConsumerRecord[]|kafka:Error records = consumer->poll(POLL_TIMEOUT_SECONDS);
        if records is kafka:Error {
            log:printError("Kafka poll failed", records);
            continue;
        }
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            do {
                string raw = check string:fromBytes(rec.value);
                json payload = check raw.fromJsonString();
                string? orderId = getString(payload, "orderId");
                if orderId is () {
                    log:printWarn("Event without orderId ignored", topic = topic);
                } else {
                    dispatchEvent(topic, orderId);
                }
            } on fail error e {
                log:printError("Failed to process event", e, topic = topic);
            }
        }
    }
}

function dispatchEvent(string topic, string orderId) {
    match topic {
        "payments.completed" => {
            onPaymentCompleted(orderId);
        }
        "payments.failed" => {
            onPaymentFailed(orderId);
        }
        "delivery.assigned" => {
            onDeliveryAssigned(orderId);
        }
        "delivery.picked_up" => {
            onDeliveryPickedUp(orderId);
        }
        "delivery.delivered" => {
            onDeliveryCompleted(orderId);
        }
        _ => {
            log:printWarn("Unhandled topic", topic = topic);
        }
    }
}

function applyEventStatus(string orderId, string newStatus, string? reason = ()) {
    error? result = updateOrderStatus(orderId, newStatus, reason);
    if result is error {
        log:printError("Status update from event failed", result, orderId = orderId, newStatus = newStatus);
    }
}

// CREATED -> CONFIRMED
function onPaymentCompleted(string orderId) {
    applyEventStatus(orderId, CONFIRMED);
}

// Cancel order
function onPaymentFailed(string orderId) {
    applyEventStatus(orderId, CANCELLED, "payment failed");
}

// Update to OUT_FOR_DELIVERY if applicable (only once the order is READY)
function onDeliveryAssigned(string orderId) {
    Order|error o = fetchOrder(orderId);
    if o is Order && o.status == READY {
        applyEventStatus(orderId, OUT_FOR_DELIVERY);
    } else {
        log:printInfo("Delivery assigned, order not ready yet", orderId = orderId);
    }
}

// Update to OUT_FOR_DELIVERY (no-op if already set)
function onDeliveryPickedUp(string orderId) {
    applyEventStatus(orderId, OUT_FOR_DELIVERY);
}

// DELIVERED
function onDeliveryCompleted(string orderId) {
    applyEventStatus(orderId, DELIVERED);
}

//  Helpers
function getString(json payload, string key) returns string? {
    if payload is map<json> {
        json v = payload[key];
        if v is string {
            return v;
        }
    }
    return ();
}

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
    if e is InvalidTransitionError {
        return jsonResponse(http:STATUS_CONFLICT, {message: e.message()});
    }
    log:printError("Request failed", e);
    return jsonResponse(http:STATUS_INTERNAL_SERVER_ERROR, {message: "Internal server error"});
}

// Shared by cancel / confirm / prepare / ready
function changeStatusResponse(string orderId, string newStatus, string? reason = ()) returns http:Response {
    error? result = updateOrderStatus(orderId, newStatus, reason);
    if result is error {
        return errorResponse(result);
    }
    Order|error o = fetchOrder(orderId);
    if o is error {
        return errorResponse(o);
    }
    return jsonResponse(http:STATUS_OK, o.toJson());
}

function insertOrder(Order o) returns error? {
    mongodb:Collection collection = check getOrdersCollection();
    check collection->insertOne(o);
}

// REST API 
service /orders on new http:Listener(orderServicePort) {

        // createOrder: save as CREATED, publish orders.created
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

        string now = nowString();
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

        error? saved = insertOrder(newOrder);
        if saved is error {
            return errorResponse(saved);
        }

        publishEvent(TOPIC_ORDER_CREATED, newOrder.orderId, {
            orderId: newOrder.orderId,
            customerId: newOrder.customerId,
            restaurantId: newOrder.restaurantId,
            totalAmount: newOrder.totalAmount,
            status: newOrder.status
        });
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

    // listOrdersByCustomer
    resource function get customer/[string customerId]() returns http:Response {
        Order[]|error orders = listOrders({customerId});
        if orders is error {
            return errorResponse(orders);
        }
        return jsonResponse(http:STATUS_OK, orders.toJson());
    }

    // listOrdersByRestaurant
    resource function get restaurant/[string restaurantId]() returns http:Response {
        Order[]|error orders = listOrders({restaurantId});
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
        return jsonResponse(http:STATUS_OK, {status: "UP", 'service: "order-service"});
    }
}