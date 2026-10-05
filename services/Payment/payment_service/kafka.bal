import ballerina/lang.runtime;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

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

function paymentEvent(Payment p) returns json => {
    eventId: uuid:createType4AsString(),
    orderId: p.orderId,
    paymentId: p.paymentId,
    customerId: p.customerId,
    amount: p.amount,
    status: p.status,
    reason: p.reason,
    timestamp: nowIso()
};

// payments.completed (key = orderId)
function publishPaymentCompleted(Payment p) returns error? {
    check publishEvent("payments.completed", p.orderId, paymentEvent(p));
}

// payments.failed
function publishPaymentFailed(Payment p) returns error? {
    check publishEvent("payments.failed", p.orderId, paymentEvent(p));
}

// payments.refunded
function publishPaymentRefunded(Payment p) returns error? {
    check publishEvent("payments.refunded", p.orderId, paymentEvent(p));
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

// New order -> charge it and publish the result
function onOrderCreated(string orderId, json event) returns error? {
    Payment? existing = check findPaymentByOrder(orderId);
    if existing is Payment {
        // Duplicate, or a retry after the publish failed: just re-announce the stored outcome
        if existing.status == COMPLETED {
            check publishPaymentCompleted(existing);
        } else if existing.status == FAILED {
            check publishPaymentFailed(existing);
        }
        return;
    }
    _ = check processPayment(
        orderId,
        getOptString(event, "customerId") ?: "",
        getOptDecimal(event, "totalAmount"),
        getOptString(event, "paymentMethod") ?: "CARD"
    );
}

// Cancelled order -> refund if it was already paid
function onOrderCancelled(string orderId, json event) returns error? {
    Payment? p = check findPaymentByOrder(orderId);
    if p is () {
        // Cancel beat orders.created (or the payment): leave a FAILED record so we never charge it
        string now = nowIso();
        check insertPayment({
            paymentId: uuid:createType4AsString(),
            orderId: orderId,
            customerId: getOptString(event, "customerId") ?: "",
            amount: 0d,
            status: FAILED,
            reason: "order cancelled before payment",
            createdAt: now,
            updatedAt: now
        });
        return;
    }
    if p.status == COMPLETED {
        _ = check refundPayment(p);
        log:printInfo("Refunded payment " + p.paymentId + " for cancelled order " + orderId);
    }
}
