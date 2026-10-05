import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

final kafka:Producer producer = check new (kafkaBootstrap, {
    clientId: groupId + "-producer",
    acks: "all",
    retryCount: 3
});

function publishEvent(string topic, string key, json payload) returns error? {
    check producer->send({
        topic: topic,
        key: key.toBytes(),
        value: payload.toJsonString().toBytes()
    });
}

listener kafka:Listener orderListener = new (kafkaBootstrap, {
    groupId: groupId,
    topics: ["orders.status.changed", "orders.cancelled"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on orderListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            do {
                json event = check parseEvent(rec.value);
                check handleEvent(topic, event);
            } on fail error e {
                log:printError("Failed to process event from " + topic, 'error = e);
                error? d = producer->send({topic: topic + ".dlq", value: rec.value});
                if d is error {
                    log:printError("Could not publish to DLQ", 'error = d);
                }
            }
        }
    }
}

function handleEvent(string topic, json event) returns error? {
    string orderId = check getString(event, "orderId");

    if topic == "orders.status.changed" {
        string status = getOptString(event, "newStatus") ?: getOptString(event, "status") ?: "";
        if status != "READY" {
            return;
        }
        if !(check markProcessed(eventKey(topic, event))) {
            return;
        }
        // Order is READY -> create a delivery job and try to dispatch a driver
        Delivery? existing = check findDelivery(orderId);
        if existing is Delivery {
            return;
        }
        Delivery d = {
            deliveryId: uuid:createType4AsString(),
            orderId: orderId,
            customerId: getOptString(event, "customerId") ?: "",
            restaurantId: getOptString(event, "restaurantId") ?: "",
            status: PENDING_DRIVER,
            createdAt: nowIso(),
            updatedAt: nowIso()
        };
        check insertDelivery(d);
        boolean assigned = check assignDriverTo(d);
        if !assigned {
            log:printInfo("No driver free, order " + orderId + " waiting in PENDING_DRIVER");
        }
    } else if topic == "orders.cancelled" {
        if !(check markProcessed(eventKey(topic, event))) {
            return;
        }
        Delivery? d = check findDelivery(orderId);
        if d is Delivery && d.status != DELIVERED && d.status != CANCELLED {
            check updateDelivery(orderId, {status: CANCELLED});
            if d.driverId != "" {
                check setAvailability(d.driverId, true);
                check assignPending();
            }
        }
    }
}

// Give a free driver to this delivery and announce it on delivery.assigned
function assignDriverTo(Delivery d) returns boolean|error {
    Driver? drv = check claimDriver();
    if drv is () {
        return false;
    }
    check updateDelivery(d.orderId, {driverId: drv.driverId, status: ASSIGNED});
    check publishEvent("delivery.assigned", d.orderId, {
        eventId: uuid:createType4AsString(),
        orderId: d.orderId,
        customerId: d.customerId,
        restaurantId: d.restaurantId,
        driverId: drv.driverId,
        driverName: drv.name,
        timestamp: nowIso()
    });
    log:printInfo("Order " + d.orderId + " assigned to driver " + drv.driverId);
    return true;
}

// Called whenever a driver becomes free: serve waiting orders (oldest first)
function assignPending() returns error? {
    Delivery[] waiting = check listDeliveries(PENDING_DRIVER);
    foreach Delivery d in waiting {
        boolean ok = check assignDriverTo(d);
        if !ok {
            break;
        }
    }
}
