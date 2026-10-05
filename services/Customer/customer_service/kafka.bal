import ballerina/log;
import ballerinax/kafka;

final kafka:Producer producer = check new (kafkaBootstrap, {
    clientId: groupId + "-producer",
    acks: "all",
    retryCount: 3
});

listener kafka:Listener orderListener = new (kafkaBootstrap, {
    groupId: groupId,
    topics: ["orders.created", "orders.status.changed", "orders.cancelled"],
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
    if !(check markProcessed(eventKey(topic, event))) {
        return;
    }
    match topic {
        "orders.created" => {
            string customerId = check getString(event, "customerId");
            OrderHistoryEntry? existing = check findOrderEntry(orderId);
            if existing is () {
                check insertOrderEntry({
                    orderId: orderId,
                    customerId: customerId,
                    restaurantId: getOptString(event, "restaurantId") ?: "",
                    totalAmount: getOptFloat(event, "totalAmount"),
                    status: "CREATED",
                    createdAt: nowIso(),
                    updatedAt: nowIso()
                });
            }
        }
        "orders.status.changed" => {
            string status = getOptString(event, "newStatus") ?: getOptString(event, "status") ?: "";
            if status != "" {
                check updateOrderStatus(orderId, status);
            }
        }
        "orders.cancelled" => {
            check updateOrderStatus(orderId, "CANCELLED");
        }
    }
    log:printInfo("Processed " + topic + " for order " + orderId);
}
