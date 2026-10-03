import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int deliveryServicePort = ?;

final kafka:Producer deliveryEventProducer = check new (kafkaBroker);
final kafka:Consumer orderReadyConsumer = check new (kafkaBroker, groupId = "delivery-service");
final mongodb:Client dbClient = check new (mongoUri);

// ---------- Kafka client setup ----------
function getKafkaProducer() returns kafka:Producer {
    // return producer instance
}

function getKafkaConsumer() returns kafka:Consumer {
    // return consumer instance
}

function getDbClient() returns mongodb:Client {
    // return db client
}

// ---------- Kafka consumer ----------
function onOrderReady() {
    // trigger findAvailableDriver + assignDriver
}

// ---------- Driver assignment ----------
function findAvailableDriver(string restaurantId) returns string? {
    // pick an available driver
}

function handleNoDriverAvailable(string orderId) {
    // retry or queue
}

// ---------- Kafka producers ----------
function publishDeliveryAssigned(string orderId, string driverId) {
    // publish to delivery.assigned
}

function publishDeliveryCompleted(string orderId, string driverId) {
    // publish to delivery.completed
}

// ---------- REST API ----------
service /deliveries on new http:Listener(deliveryServicePort) {

    resource function post drivers(http:Request req) returns http:Response {
        // registerDriver
    }

    resource function put drivers/[string driverId]/status(http:Request req) returns http:Response {
        // updateDriverStatus (AVAILABLE/BUSY/OFFLINE)
    }

    resource function post assign(http:Request req) returns http:Response {
        // assignDriver
    }

    resource function put [string deliveryId]/status(http:Request req) returns http:Response {
        // updateDeliveryStatus
    }

    resource function get [string deliveryId]() returns http:Response {
        // getDelivery
    }

    resource function get [string deliveryId]/track() returns http:Response {
        // trackDelivery
    }

    resource function post [string deliveryId]/complete() returns http:Response {
        // completeDelivery
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}