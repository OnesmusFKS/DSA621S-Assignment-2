import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int orderServicePort = ?;

final kafka:Producer orderEventProducer = check new (kafkaBroker);
final kafka:Consumer paymentConsumer = check new (kafkaBroker, groupId = "order-service-payment");
final kafka:Consumer deliveryConsumer = check new (kafkaBroker, groupId = "order-service-delivery");
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

// ---------- State machine ----------
function isValidTransition(string currentStatus, string newStatus) returns boolean {
    // validate CREATED -> CONFIRMED -> PREPARING -> READY -> OUT_FOR_DELIVERY -> DELIVERED (or CANCELLED)
}

function updateOrderStatus(string orderId, string newStatus) returns error? {
    // the only function that changes status
}

// ---------- Kafka producers ----------
function publishStatusChanged(string orderId, string newStatus) {
    // publish to orders.status.changed
}

// ---------- Kafka consumers ----------
function onPaymentCompleted() {
    // CREATED -> CONFIRMED
}

function onPaymentFailed() {
    // cancel order
}

function onDeliveryAssigned() {
    // update to OUT_FOR_DELIVERY if applicable
}

function onDeliveryPickedUp() {
    // update to OUT_FOR_DELIVERY
}

function onDeliveryCompleted() {
    // DELIVERED
}

// ---------- REST API ----------
service /orders on new http:Listener(orderServicePort) {

    resource function post create(http:Request req) returns http:Response {
        // createOrder: save as CREATED, publish orders.created
    }

    resource function get [string orderId]() returns http:Response {
        // getOrder
    }

    resource function get customer/[string customerId]() returns http:Response {
        // listOrdersByCustomer
    }

    resource function get restaurant/[string restaurantId]() returns http:Response {
        // listOrdersByRestaurant
    }

    resource function post [string orderId]/cancel() returns http:Response {
        // cancelOrder: only allowed in early states
    }

    resource function post [string orderId]/confirm() returns http:Response {
        // confirmOrder
    }

    resource function post [string orderId]/prepare() returns http:Response {
        // startPreparing
    }

    resource function post [string orderId]/ready() returns http:Response {
        // markReady
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}