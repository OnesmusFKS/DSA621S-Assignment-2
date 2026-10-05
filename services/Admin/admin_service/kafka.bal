import ballerina/log;
import ballerinax/kafka;

final kafka:Producer producer = check new (kafkaBootstrap, {
    clientId: groupId + "-producer",
    acks: "all",
    retryCount: 3
});

listener kafka:Listener eventListener = new (kafkaBootstrap, {
    groupId: groupId,
    topics: [
        "orders.created",
        "orders.status.changed",
        "orders.cancelled",
        "payments.completed",
        "delivery.assigned",
        "delivery.completed"
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

service on eventListener {
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

function copyStr(json event, map<json> fields, string key) {
    string? v = getOptString(event, key);
    if v is string && v != "" {
        fields[key] = v;
    }
}

function handleEvent(string topic, json event) returns error? {
    string orderId = check getString(event, "orderId");
    if !(check markProcessed(eventKey(topic, event))) {
        return;
    }
    check ensureSummary(orderId);
    map<json> f = {};
    match topic {
        "orders.created" => {
            f["status"] = "CREATED";
            f["createdAt"] = nowIso();
            f["totalAmount"] = getOptFloat(event, "totalAmount");
            copyStr(event, f, "restaurantId");
            copyStr(event, f, "customerId");
        }
        "orders.status.changed" => {
            string status = getOptString(event, "newStatus") ?: getOptString(event, "status") ?: "";
            if status == "" {
                return;
            }
            f["status"] = status;
        }
        "orders.cancelled" => {
            f["status"] = "CANCELLED";
        }
        "payments.completed" => {
            f["paid"] = true;
        }
        "delivery.assigned" => {
            f["assignedAt"] = nowIso();
            copyStr(event, f, "driverId");
        }
        "delivery.completed" => {
            f["deliveredAt"] = nowIso();
            f["status"] = "DELIVERED";
            copyStr(event, f, "driverId");
        }
        _ => {
            return;
        }
    }
    check updateSummary(orderId, f);
    log:printInfo("Recorded " + topic + " for order " + orderId);
}
