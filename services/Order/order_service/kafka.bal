import ballerina/lang.runtime;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

const string TOPIC_ORDER_CREATED = "orders.created";
const string TOPIC_ORDER_STATUS_CHANGED = "orders.status.changed";
const string TOPIC_ORDER_CONFIRMED = "orders.confirmed";
const string TOPIC_ORDER_CANCELLED = "orders.cancelled";
const int MAX_ATTEMPTS = 3;

final kafka:Producer producer = check new (kafkaBootstrap, {
    clientId: groupId + "-producer",
    acks: "all",
    retryCount: 3
});

// ---------- producers ----------
function publishEvent(string topic, string key, json payload) {
    kafka:Error? sent = producer->send({
        topic: topic,
        key: key.toBytes(),
        value: payload.toJsonString().toBytes()
    });
    if sent is kafka:Error {
        log:printError("Failed to publish event", sent, topic = topic, key = key);
    }
}

// orders.created (key = orderId)
function publishOrderCreated(Order o) {
    publishEvent(TOPIC_ORDER_CREATED, o.orderId, {
        eventId: uuid:createType4AsString(),
        orderId: o.orderId,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        items: o.items.toJson(),
        totalAmount: o.totalAmount,
        status: o.status,
        timestamp: o.createdAt
    });
}

// A fresh eventId per publish: consumers dedupe on it, and the same order
// event goes to several topics (status.changed + confirmed/cancelled).
function statusEvent(Order o, string previousStatus, string? reason) returns json => {
    eventId: uuid:createType4AsString(),
    orderId: o.orderId,
    customerId: o.customerId,
    restaurantId: o.restaurantId,
    status: o.status,
    previousStatus: previousStatus,
    reason: reason,
    totalAmount: o.totalAmount,
    timestamp: o.updatedAt
};

// orders.status.changed for every transition; confirmed/cancelled also go to their own topics
function publishStatusChanged(Order o, string previousStatus, string? reason = ()) {
    publishEvent(TOPIC_ORDER_STATUS_CHANGED, o.orderId, statusEvent(o, previousStatus, reason));
    if o.status == CONFIRMED {
        publishEvent(TOPIC_ORDER_CONFIRMED, o.orderId, statusEvent(o, previousStatus, reason));
    } else if o.status == CANCELLED {
        publishEvent(TOPIC_ORDER_CANCELLED, o.orderId, statusEvent(o, previousStatus, reason));
    }
}

// <topic>.dlq with the failure reason and the original payload
function publishToDlq(string topic, byte[] raw, string reason) {
    string|error original = string:fromBytes(raw);
    json dlq = {
        originalTopic: topic,
        reason: reason,
        payload: original is string ? original : "<unreadable bytes>",
        failedAt: nowIso()
    };
    kafka:Error? sent = producer->send({topic: topic + ".dlq", value: dlq.toJsonString().toBytes()});
    if sent is kafka:Error {
        log:printError("Could not publish to DLQ", sent, topic = topic);
    }
}

// ---------- consumer ----------
listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: groupId,
    topics: ["payments.completed", "payments.failed", "delivery.assigned", "delivery.pickedup", "delivery.completed", "stock.rejected"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on eventListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            do {
                json event = check parseEvent(rec.value);
                string key = eventKey(topic, event);
                boolean seen = check isProcessed(key);
                if seen {
                    continue;
                }
                check handleWithRetry(topic, event);
                check markProcessed(key);
            } on fail error e {
                log:printError("Failed to process event from " + topic, 'error = e);
                publishToDlq(topic, rec.value, e.message());
            }
        }
    }
}

// Retry a failing handler with a growing delay before giving up (-> DLQ)
function handleWithRetry(string topic, json event) returns error? {
    error? lastError = ();
    foreach int attempt in 1 ... MAX_ATTEMPTS {
        error? result = handleEvent(topic, event);
        if result is () {
            return;
        }
        lastError = result;
        log:printWarn("Handler failed", 'error = result, topic = topic, attempt = attempt);
        if attempt < MAX_ATTEMPTS {
            runtime:sleep(<decimal>attempt * 0.5d);
        }
    }
    return lastError;
}

function handleEvent(string topic, json event) returns error? {
    string orderId = check getString(event, "orderId");
    match topic {
        "payments.completed" => {
            check onPaymentCompleted(orderId);
        }
        "payments.failed" => {
            check onPaymentFailed(orderId, getOptString(event, "reason"));
        }
        "stock.rejected" => {
            check onStockRejected(orderId, getOptString(event, "reason"));
        }
        "delivery.assigned" => {
            check onDeliveryAssigned(orderId, getOptString(event, "driverId"));
        }
        "delivery.pickedup" => {
            check onDeliveryPickedUp(orderId);
        }
        "delivery.completed" => {
            check onDeliveryCompleted(orderId);
        }
        _ => {
            log:printWarn("Unhandled topic", topic = topic);
        }
    }
    log:printInfo("Processed " + topic + " for order " + orderId);
}

// A transition that is no longer allowed (e.g. payment arrives for an already
// cancelled order) will never succeed on retry, so log it and move on.
function applyEventStatus(string orderId, string newStatus, string? reason = ()) returns error? {
    error? result = updateOrderStatus(orderId, newStatus, reason, "kafka");
    if result is InvalidTransitionError {
        log:printWarn("Ignored event: " + result.message(), orderId = orderId);
        return;
    }
    return result;
}

// CREATED -> CONFIRMED
function onPaymentCompleted(string orderId) returns error? {
    check applyEventStatus(orderId, CONFIRMED);
}

// -> CANCELLED
function onPaymentFailed(string orderId, string? reason) returns error? {
    check applyEventStatus(orderId, CANCELLED, reason ?: "payment failed");
}

// Restaurant closed / out of stock -> CANCELLED (also valid after payment: CONFIRMED -> CANCELLED,
// which triggers orders.cancelled so Payment refunds)
function onStockRejected(string orderId, string? reason) returns error? {
    check applyEventStatus(orderId, CANCELLED, "stock rejected: " + (reason ?: "unavailable"));
}

// Only record the driver; the status moves when the driver picks the food up
function onDeliveryAssigned(string orderId, string? driverId) returns error? {
    if driverId is string {
        check setOrderDriver(orderId, driverId);
    }
}

// READY -> OUT_FOR_DELIVERY
function onDeliveryPickedUp(string orderId) returns error? {
    check applyEventStatus(orderId, OUT_FOR_DELIVERY);
}

// OUT_FOR_DELIVERY -> DELIVERED
function onDeliveryCompleted(string orderId) returns error? {
    check applyEventStatus(orderId, DELIVERED);
}
