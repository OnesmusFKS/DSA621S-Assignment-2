import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int paymentServicePort = ?;

final kafka:Producer paymentEventProducer = check new (kafkaBroker);
final kafka:Consumer orderCreatedConsumer = check new (kafkaBroker, groupId = "payment-service");
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
function onOrderCreated() {
    // trigger processPayment
}

// ---------- Idempotency ----------
function checkDuplicatePayment(string orderId) returns boolean {
    // check if a payment already exists for this order
}

// ---------- Kafka producers ----------
function publishPaymentCompleted(string orderId, string paymentId) {
    // publish to payments.completed
}

function publishPaymentFailed(string orderId, string reason) {
    // publish to payments.failed
}

// ---------- REST API ----------
service /payments on new http:Listener(paymentServicePort) {

    resource function post process(http:Request req) returns http:Response {
        // processPayment: simulate success/failure
    }

    resource function get [string paymentId]() returns http:Response {
        // getPayment
    }

    resource function get order/[string orderId]() returns http:Response {
        // getPaymentByOrder
    }

    resource function post [string paymentId]/refund() returns http:Response {
        // refundPayment: for cancelled orders
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}