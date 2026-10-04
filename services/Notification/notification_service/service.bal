import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int notificationServicePort = ?;
configurable string mongoDatabase = "notification_db";

const string NOTIFICATIONS_COLLECTION = "notifications";
const string TOPIC_PATTERN = "(orders|payments|delivery)\\..*";
const decimal POLL_TIMEOUT_SECONDS = 1;

type Notification record {
    string recipientId;
    string recipientType;
    string eventType;
    string message;
    string createdAt;
};

final kafka:Consumer eventConsumer = check new (kafkaBroker,
    groupId = "notification-service",
    autoCommit = true,
    offsetReset = kafka:OFFSET_RESET_LATEST
);
final mongodb:Client dbClient = check new ({connection: mongoUri});

function init() returns error? {
    check eventConsumer->subscribeWithPattern(TOPIC_PATTERN);
    _ = start onAnyEvent();
}

// Kafka client setup 
function getKafkaConsumer() returns kafka:Consumer {
    return eventConsumer;
}

function getDbClient() returns mongodb:Client {
    return dbClient;
}

function getCollection() returns mongodb:Collection|error {
    mongodb:Database db = check dbClient->getDatabase(mongoDatabase);
    return db->getCollection(NOTIFICATIONS_COLLECTION);
}

// Kafka consumer (multiple topics)
function onAnyEvent() {
    kafka:Consumer consumer = eventConsumer;
    log:printInfo("Notification consumer started");
    while true {
        kafka:BytesConsumerRecord[]|error records = consumer->poll(POLL_TIMEOUT_SECONDS);
        if records is error {
            log:printError("Kafka poll failed", records);
            continue;
        }
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            do {
                string raw = check string:fromBytes(rec.value);
                json payload = check raw.fromJsonString();
                handleEvent(topic, payload);
            } on fail error e {
                log:printError("Failed to process event", e, topic = topic);
            }
        }
    }
}

function handleEvent(string eventType, json payload) {
    string? customerId = getString(payload, "customerId");
    string? restaurantId = getString(payload, "restaurantId");
    string? driverId = getString(payload, "driverId");

    match eventType {
        "orders.created"|"orders.confirmed"|"orders.cancelled" => {
            if customerId is string {
                notifyCustomer(customerId, buildMessage(eventType, payload, "customer"), eventType);
            }
            if restaurantId is string {
                notifyRestaurant(restaurantId, buildMessage(eventType, payload, "restaurant"), eventType);
            }
        }
        "payments.completed"|"payments.failed"|"payments.refunded" => {
            if customerId is string {
                notifyCustomer(customerId, buildMessage(eventType, payload, "customer"), eventType);
            }
            if restaurantId is string && eventType == "payments.completed" {
                notifyRestaurant(restaurantId, buildMessage(eventType, payload, "restaurant"), eventType);
            }
        }
        "delivery.assigned"|"delivery.picked_up"|"delivery.delivered" => {
            if customerId is string {
                notifyCustomer(customerId, buildMessage(eventType, payload, "customer"), eventType);
            }
            if driverId is string && eventType == "delivery.assigned" {
                notifyDriver(driverId, buildMessage(eventType, payload, "driver"), eventType);
            }
            if restaurantId is string && eventType == "delivery.picked_up" {
                notifyRestaurant(restaurantId, buildMessage(eventType, payload, "restaurant"), eventType);
            }
        }
        _ => {
            log:printWarn("Unhandled event type", eventType = eventType);
        }
    }
}

function getString(json payload, string key) returns string? {
    if payload is map<json> {
        json v = payload[key];
        if v is string {
            return v;
        }
    }
    return ();
}

// Message building 
function buildMessage(string eventType, json payload, string recipientType) returns string {
    string orderId = getString(payload, "orderId") ?: "unknown";
    string reason = getString(payload, "reason") ?: "no reason provided";

    match [eventType, recipientType] {
        ["orders.created", "customer"] => {
            return string `Your order ${orderId} has been placed.`;
        }
        ["orders.created", "restaurant"] => {
            return string `New order ${orderId} received. Please confirm.`;
        }
        ["orders.confirmed", "customer"] => {
            return string `Your order ${orderId} was confirmed and is being prepared.`;
        }
        ["orders.confirmed", "restaurant"] => {
            return string `Order ${orderId} confirmed. Start preparing.`;
        }
        ["orders.cancelled", "customer"] => {
            return string `Your order ${orderId} was cancelled (${reason}).`;
        }
        ["orders.cancelled", "restaurant"] => {
            return string `Order ${orderId} was cancelled (${reason}).`;
        }
        ["payments.completed", "customer"] => {
            return string `Payment for order ${orderId} was successful.`;
        }
        ["payments.completed", "restaurant"] => {
            return string `Payment received for order ${orderId}.`;
        }
        ["payments.failed", "customer"] => {
            return string `Payment for order ${orderId} failed. Please try another method.`;
        }
        ["payments.refunded", "customer"] => {
            return string `Your payment for order ${orderId} has been refunded.`;
        }
        ["delivery.assigned", "customer"] => {
            return string `A driver has been assigned to your order ${orderId}.`;
        }
        ["delivery.assigned", "driver"] => {
            return string `New delivery assigned: order ${orderId}.`;
        }
        ["delivery.picked_up", "customer"] => {
            return string `Your order ${orderId} is on its way.`;
        }
        ["delivery.picked_up", "restaurant"] => {
            return string `Order ${orderId} was picked up by the driver.`;
        }
        ["delivery.delivered", "customer"] => {
            return string `Your order ${orderId} was delivered. Enjoy your meal!`;
        }
        _ => {
            return string `Update on order ${orderId}: ${eventType}`;
        }
    }
}

// Channel dispatch
function notifyCustomer(string customerId, string message, string eventType = "") {
    dispatch(customerId, "customer", eventType, message);
}

function notifyRestaurant(string restaurantId, string message, string eventType = "") {
    dispatch(restaurantId, "restaurant", eventType, message);
}

function notifyDriver(string driverId, string message, string eventType = "") {
    dispatch(driverId, "driver", eventType, message);
}

function dispatch(string recipientId, string recipientType, string eventType, string message) {
    sendEmail(recipientId, message);
    sendSMS(recipientId, message);
    sendPush(recipientId, message);

    error? saved = saveNotification(recipientId, message, recipientType, eventType);
    if saved is error {
        log:printError("Failed to persist notification", saved, recipientId = recipientId);
    }
}

function sendEmail(string recipient, string message) {
    log:printInfo("[EMAIL] sent", recipient = recipient, text = message);
}

function sendSMS(string recipient, string message) {
    log:printInfo("[SMS] sent", recipient = recipient, text = message);
}

function sendPush(string recipient, string message) {
    log:printInfo("[PUSH] sent", recipient = recipient, text = message);
}

// Persistence 
function saveNotification(string recipientId, string message,
        string recipientType = "unknown", string eventType = "") returns error? {
    mongodb:Collection collection = check getCollection();
    Notification notification = {
        recipientId,
        recipientType,
        eventType,
        message,
        createdAt: time:utcToString(time:utcNow())
    };
    check collection->insertOne(notification);
}

function getNotifications(string recipientId) returns Notification[]|error {
    mongodb:Collection collection = check getCollection();
    stream<Notification, error?> results = check collection->find({recipientId});
    return from Notification n in results
        select n;
}

// REST API 
service /notifications on new http:Listener(notificationServicePort) {

    resource function get [string recipientId]() returns http:Response {
        http:Response res = new;
        Notification[]|error notifications = getNotifications(recipientId);
        if notifications is error {
            log:printError("Failed to fetch notifications", notifications, recipientId = recipientId);
            res.statusCode = http:STATUS_INTERNAL_SERVER_ERROR;
            res.setJsonPayload({message: "Failed to fetch notifications"});
            return res;
        }
        res.statusCode = http:STATUS_OK;
        res.setJsonPayload(notifications.toJson());
        return res;
    }

    resource function get health() returns http:Response {
        http:Response res = new;
        res.statusCode = http:STATUS_OK;
        res.setJsonPayload({status: "UP", 'service: "notification-service"});
        return res;
    }
}