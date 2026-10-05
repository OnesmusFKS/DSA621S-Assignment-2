import ballerina/http;
import ballerina/kafka;
import ballerina/mongodb;
import ballerina/log;

configurable string kafkaBroker = ?;
configurable string mongoUri = ?;
configurable int restaurantServicePort = ?;

final kafka:Producer stockResultProducer = check new (kafkaBroker);
final kafka:Consumer orderCreatedConsumer = check new (kafkaBroker, groupId = "restaurant-service");
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

// ---------- Kafka consumer: order created ----------
function onOrderCreated() {
    // validate restaurant is open and stock is available, then reserve it
    // call publishStockResult
}

// ---------- Kafka producer: stock result ----------
function publishStockResult(string orderId, boolean reserved) {
    // publish to stock.reserved or stock.rejected topic
}

// ---------- REST API ----------
service /restaurants on new http:Listener(restaurantServicePort) {

    resource function post registerRestaurant(http:Request req) returns http:Response {
        // registerRestaurant
    }

    resource function get [string restaurantId]() returns http:Response {
        // getRestaurant
    }

    resource function get list() returns http:Response {
        // listRestaurants
    }

    resource function post [string restaurantId]/menu(http:Request req) returns http:Response {
        // addMenuItem
    }

    resource function put [string restaurantId]/menu/[string itemId](http:Request req) returns http:Response {
        // updateMenuItem
    }

    resource function delete [string restaurantId]/menu/[string itemId]() returns http:Response {
        // removeMenuItem
    }

    resource function get [string restaurantId]/menu() returns http:Response {
        // getMenu
    }

    resource function put [string restaurantId]/hours(http:Request req) returns http:Response {
        // setOpeningHours
    }

    resource function get [string restaurantId]/is-open() returns http:Response {
        // isOpenNow
    }

    resource function get [string restaurantId]/stock/[string itemId]() returns http:Response {
        // checkStock
    }

    resource function post [string restaurantId]/stock/reserve(http:Request req) returns http:Response {
        // reserveStock
    }

    resource function post [string restaurantId]/stock/release(http:Request req) returns http:Response {
        // releaseStock
    }

    resource function get health() returns http:Response {
        // healthCheck
    }
}