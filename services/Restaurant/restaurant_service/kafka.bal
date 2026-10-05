import ballerina/lang.runtime;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

const string STOCK_RESERVED_TOPIC = "stock.reserved";
const string STOCK_REJECTED_TOPIC = "stock.rejected";
const int MAX_ATTEMPTS = 3;

final kafka:Producer producer = check new (kafkaBootstrap, {
    clientId: groupId + "-producer",
    acks: "all",
    retryCount: 3
});

// ---------- producers ----------
function publishEvent(string topic, string key, json payload) returns error? {
    check producer->send({
        topic: topic,
        key: key.toBytes(),
        value: payload.toJsonString().toBytes()
    });
}

// stock.reserved / stock.rejected (key = orderId)
function publishStockResult(string orderId, string restaurantId, boolean reserved, string reason) returns error? {
    string topic = reserved ? STOCK_RESERVED_TOPIC : STOCK_REJECTED_TOPIC;
    check publishEvent(topic, orderId, {
        eventId: uuid:createType4AsString(),
        orderId: orderId,
        restaurantId: restaurantId,
        reserved: reserved,
        reason: reason,
        timestamp: nowIso()
    });
    log:printInfo(string `Published ${topic} for order ${orderId}`);
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
listener kafka:Listener orderListener = new (kafkaBootstrap, {
    groupId: groupId,
    topics: ["orders.created", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on orderListener {
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
        "orders.created" => {
            check onOrderCreated(orderId, event);
        }
        "orders.cancelled" => {
            check onOrderCancelled(orderId, event);
        }
        _ => {
            log:printWarn("Unhandled topic", topic = topic);
        }
    }
    log:printInfo("Processed " + topic + " for order " + orderId);
}

// New order -> check opening hours + reserve stock, then announce the result
function onOrderCreated(string orderId, json event) returns error? {
    StockReservation? existing = check findReservation(orderId);
    if existing is StockReservation {
        // Duplicate, a retry after a failed publish, or the order was cancelled first:
        // never reserve twice, just re-announce the stored decision
        check publishStockResult(orderId, existing.restaurantId, existing.reserved, existing.reason);
        return;
    }

    OrderCreated created = check event.cloneWithType();
    StockDecision decision = check processOrder(created);
    check insertReservation({
        orderId: orderId,
        restaurantId: created.restaurantId,
        items: created.items,
        reserved: decision.reserved,
        reason: decision.reason,
        createdAt: nowIso()
    });
    check publishStockResult(orderId, created.restaurantId, decision.reserved, decision.reason);
}

// Cancelled order -> give the reserved stock back (once)
function onOrderCancelled(string orderId, json event) returns error? {
    StockReservation? r = check findReservation(orderId);
    if r is () {
        // Cancel beat orders.created: leave a tombstone so we never reserve for it
        check insertReservation({
            orderId: orderId,
            restaurantId: getOptString(event, "restaurantId") ?: "",
            items: [],
            reserved: false,
            reason: "order cancelled before stock check",
            released: true,
            createdAt: nowIso()
        });
        return;
    }
    if r.reserved && !r.released {
        check releaseStock(r.restaurantId, r.items);
        check markReservationReleased(orderId);
        log:printInfo("Released stock for cancelled order " + orderId);
    }
}
