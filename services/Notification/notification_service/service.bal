import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int notificationServicePort = ?;

final kafka:Consumer eventConsumer = check new (kafkaBroker, groupId = "notification-service");
final mongodb:Client dbClient = check new (mongoUri);

// ---------- Kafka client setup ----------
function getKafkaConsumer() returns kafka:Consumer {
    // return consumer instance
}

function getDbClient() returns mongodb:Client {
    // return db client
}

// ---------- Kafka consumer (multiple topics) ----------
function onAnyEvent() {
    // subscribe to orders.*, payments.*, delivery.*
    // buildMessage -> notifyCustomer / notifyRestaurant / notifyDriver
}

// ---------- Message building ----------
function buildMessage(string eventType, json payload, string recipientType) returns string {
    // template per event and recipient type
}

// ---------- Channel dispatch ----------
function notifyCustomer(string customerId, string message) {
    // sendEmail / sendSMS / sendPush
}

function notifyRestaurant(string restaurantId, string message) {
    // sendEmail / sendSMS / sendPush
}

function notifyDriver(string driverId, string message) {
    // sendEmail / sendSMS / sendPush
}

function sendEmail(string recipient, string message) {
    // simulated email channel
}

function sendSMS(string recipient, string message) {
    // simulated SMS channel
}

function sendPush(string recipient, string message) {
    // simulated push channel
}

// ---------- Persistence ----------
function saveNotification(string recipientId, string message) returns error? {
    // log notification to DB
}

// ---------- REST API ----------
service /notifications on new http:Listener(notificationServicePort) {

    resource function get [string recipientId]() returns http:Response {
        // getNotifications
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}