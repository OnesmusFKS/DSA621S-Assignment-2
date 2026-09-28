import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int adminServicePort = ?;

final kafka:Consumer eventConsumer = check new (kafkaBroker, groupId = "admin-service");
final mongodb:Client dbClient = check new (mongoUri);

// ---------- Kafka client setup ----------
function getKafkaConsumer() returns kafka:Consumer {
    // return consumer instance
}

function getDbClient() returns mongodb:Client {
    // return db client
}

// ---------- Kafka consumer (read-model builder) ----------
function onAnyEvent() {
    // subscribe to orders.*, payments.*, delivery.*
    // build aggregates so Admin doesn't query other services' DBs
}

// ---------- Aggregation logic ----------
function updateRestaurantStats(json event) {
    // update orders, revenue, popular items aggregates
}

function updateDeliveryPerformance(json event) {
    // update average delivery time, driver stats
}

function updateOrderStatusBreakdown(json event) {
    // update status counts
}

// ---------- REST API ----------
service /admin on new http:Listener(adminServicePort) {

    resource function get restaurants/[string restaurantId]/stats() returns http:Response {
        // getRestaurantStats
    }

    resource function get delivery/performance() returns http:Response {
        // getDeliveryPerformance
    }

    resource function get orders/status-breakdown() returns http:Response {
        // getOrderStatusBreakdown
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}