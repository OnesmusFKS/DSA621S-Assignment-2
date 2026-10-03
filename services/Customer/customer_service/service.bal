import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int customerServicePort = ?;

final kafka:Producer orderEventProducer = check new (kafkaBroker);
final kafka:Consumer orderEventConsumer = check new (kafkaBroker, groupId = "customer-service");
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

// ---------- Kafka consumer: order events ----------
function onOrderEvent() {
    // subscribe to orders.created, orders.status.changed
    // update local order history copy
}

// ---------- REST API ----------
service /customers on new http:Listener(customerServicePort) {

    resource function post registerCustomer(http:Request req) returns http:Response {
        // create account
    }

    resource function get [string customerId]() returns http:Response {
        // getCustomer
    }

    resource function put [string customerId](http:Request req) returns http:Response {
        // updateCustomer
    }

    resource function delete [string customerId]() returns http:Response {
        // deleteCustomer
    }

    resource function post [string customerId]/addresses(http:Request req) returns http:Response {
        // addAddress
    }

    resource function get [string customerId]/addresses() returns http:Response {
        // getAddresses
    }

    resource function put [string customerId]/addresses/[string addressId](http:Request req) returns http:Response {
        // updateAddress
    }

    resource function delete [string customerId]/addresses/[string addressId]() returns http:Response {
        // removeAddress
    }

    resource function get [string customerId]/orders() returns http:Response {
        // getOrderHistory (from local copy)
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}