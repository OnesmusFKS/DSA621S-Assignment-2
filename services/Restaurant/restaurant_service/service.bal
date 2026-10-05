import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/kafka;
import ballerinax/mongodb;

configurable string kafkaBroker = "localhost:9092";
configurable string mongoUri = "mongodb://localhost:27017";
configurable string dbName = "restaurantdb";
configurable int restaurantServicePort = 9090;

const string ORDER_CREATED_TOPIC = "order.created";
const string STOCK_RESERVED_TOPIC = "stock.reserved";
const string STOCK_REJECTED_TOPIC = "stock.rejected";

// ---------- Types ----------

type OpeningHours record {|
    string openTime;   // "HH:mm", UTC
    string closeTime;  // "HH:mm", UTC (may be earlier than openTime for overnight hours)
|};

type Restaurant record {
    string restaurantId;
    string name;
    string address?;
    OpeningHours hours?;
};

type MenuItem record {
    string itemId;
    string restaurantId?;
    string name;
    decimal price;
    int stock = 0;
};

type OrderItem record {
    string itemId;
    int quantity;
};

type StockRequest record {
    OrderItem[] items;
};

type OrderCreated record {
    string orderId;
    string restaurantId;
    OrderItem[] items;
};

// ---------- Clients ----------

final mongodb:Client mongoClient = check new ({connection: mongoUri});
final kafka:Producer stockProducer = check new (kafkaBroker, {
    clientId: "restaurant-service-producer",
    acks: "all",
    retryCount: 3
});

function getCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(dbName);
    return db->getCollection(name);
}

// ---------- Restaurant data ----------

function createRestaurant(Restaurant restaurant) returns error? {
    mongodb:Collection restaurants = check getCollection("restaurants");
    check restaurants->insertOne(restaurant);
}

function findRestaurant(string restaurantId) returns Restaurant?|error {
    mongodb:Collection restaurants = check getCollection("restaurants");
    // Hide Mongo's internal _id so records bind and serialise cleanly.
    Restaurant? found = check restaurants->findOne(
        {restaurantId},
        projection = {"_id": 0},
        targetType = Restaurant
    );
    return found;
}

function listRestaurants() returns Restaurant[]|error {
    mongodb:Collection restaurants = check getCollection("restaurants");
    stream<Restaurant, error?> results = check restaurants->find(
        {},
        projection = {"_id": 0},
        targetType = Restaurant
    );
    Restaurant[] all = check from Restaurant r in results
        select r;
    return all;
}

function setOpeningHours(string restaurantId, OpeningHours hours) returns boolean|error {
    mongodb:Collection restaurants = check getCollection("restaurants");
    mongodb:UpdateResult res = check restaurants->updateOne(
        {restaurantId},
        {set: {hours: hours}}
    );
    return res.matchedCount > 0;
}

// ---------- Menu data ----------

function addMenuItem(string restaurantId, MenuItem item) returns error? {
    mongodb:Collection menu = check getCollection("menu_items");
    MenuItem toSave = {...item, restaurantId};
    check menu->insertOne(toSave);
}

function updateMenuItem(string restaurantId, string itemId, MenuItem item) returns boolean|error {
    mongodb:Collection menu = check getCollection("menu_items");
    mongodb:UpdateResult res = check menu->updateOne(
        {restaurantId, itemId},
        {set: {name: item.name, price: item.price, stock: item.stock}}
    );
    return res.matchedCount > 0;
}

function removeMenuItem(string restaurantId, string itemId) returns boolean|error {
    mongodb:Collection menu = check getCollection("menu_items");
    mongodb:DeleteResult res = check menu->deleteOne({restaurantId, itemId});
    return res.deletedCount > 0;
}

function getMenu(string restaurantId) returns MenuItem[]|error {
    mongodb:Collection menu = check getCollection("menu_items");
    stream<MenuItem, error?> results = check menu->find(
        {restaurantId},
        projection = {"_id": 0},
        targetType = MenuItem
    );
    MenuItem[] items = check from MenuItem m in results
        select m;
    return items;
}

// ---------- Opening hours ----------

function toMinutes(string hhmm) returns int|error {
    int? colon = hhmm.indexOf(":");
    if colon is () {
        return error("Invalid time '" + hhmm + "', expected HH:mm");
    }
    int hours = check int:fromString(hhmm.substring(0, colon));
    int minutes = check int:fromString(hhmm.substring(colon + 1));
    if hours < 0 || hours > 23 || minutes < 0 || minutes > 59 {
        return error("Time out of range: " + hhmm);
    }
    return hours * 60 + minutes;
}

function isOpenNow(string restaurantId) returns boolean|error {
    Restaurant? restaurant = check findRestaurant(restaurantId);
    if restaurant is () {
        return error("Restaurant not found: " + restaurantId);
    }
    OpeningHours? hours = restaurant?.hours;
    if hours is () {
        return false; // no hours set means we treat it as closed
    }

    time:Civil now = time:utcToCivil(time:utcNow());
    int current = now.hour * 60 + now.minute;
    int openAt = check toMinutes(hours.openTime);
    int closeAt = check toMinutes(hours.closeTime);

    if openAt <= closeAt {
        return current >= openAt && current < closeAt;
    }
    // Overnight hours, e.g. 18:00 -> 02:00
    return current >= openAt || current < closeAt;
}

// ---------- Stock ----------

function checkStock(string restaurantId, string itemId) returns int?|error {
    mongodb:Collection menu = check getCollection("menu_items");
    MenuItem? item = check menu->findOne(
        {restaurantId, itemId},
        projection = {"_id": 0},
        targetType = MenuItem
    );
    return item is () ? () : item.stock;
}

// Each decrement only succeeds if enough stock is left (atomic per item).
// If any item fails, everything reserved so far is put back.
function reserveStock(string restaurantId, OrderItem[] items) returns boolean|error {
    mongodb:Collection menu = check getCollection("menu_items");
    OrderItem[] reserved = [];

    foreach OrderItem item in items {
        if item.quantity <= 0 {
            check rollback(restaurantId, reserved);
            return error("Invalid quantity for item " + item.itemId);
        }
        mongodb:UpdateResult res = check menu->updateOne(
            {restaurantId, itemId: item.itemId, stock: {"$gte": item.quantity}},
            {inc: {stock: -item.quantity}}
        );
        if res.modifiedCount == 0 {
            check rollback(restaurantId, reserved);
            return false;
        }
        reserved.push(item);
    }
    return true;
}

function rollback(string restaurantId, OrderItem[] reserved) returns error? {
    if reserved.length() > 0 {
        check releaseStock(restaurantId, reserved);
    }
}

function releaseStock(string restaurantId, OrderItem[] items) returns error? {
    mongodb:Collection menu = check getCollection("menu_items");
    foreach OrderItem item in items {
        _ = check menu->updateOne(
            {restaurantId, itemId: item.itemId},
            {inc: {stock: item.quantity}}
        );
    }
}

// ---------- Kafka producer ----------

function publishStockResult(string orderId, boolean reserved) returns error? {
    string topic = reserved ? STOCK_RESERVED_TOPIC : STOCK_REJECTED_TOPIC;
    json payload = {orderId, reserved};
    check stockProducer->send({
        topic,
        key: orderId.toBytes(),
        value: payload.toJsonString().toBytes()
    });
    check stockProducer->'flush();
    log:printInfo(string `Published ${topic} for order ${orderId}`);
}

// ---------- Kafka consumer ----------

listener kafka:Listener orderListener = new (kafkaBroker, {
    groupId: "restaurant-service",
    topics: [ORDER_CREATED_TOPIC]
});

function processOrder(OrderCreated order) returns boolean|error {
    boolean open = check isOpenNow(order.restaurantId);
    if !open {
        log:printInfo(string `Restaurant ${order.restaurantId} is closed, rejecting order ${order.orderId}`);
        return false;
    }
    return reserveStock(order.restaurantId, order.items);
}

service kafka:Service on orderListener {

    remote function onConsumerRecord(OrderCreated[] orders) returns error? {
        foreach OrderCreated order in orders {
            log:printInfo("Received order created event: " + order.orderId);

            boolean reserved = false;
            boolean|error result = processOrder(order);
            if result is error {
                // Reject rather than leave the order waiting forever.
                log:printError("Could not process order " + order.orderId, result);
            } else {
                reserved = result;
            }

            error? published = publishStockResult(order.orderId, reserved);
            if published is error {
                log:printError("Could not publish stock result for order " + order.orderId, published);
            }
        }
    }
}

// ---------- REST helpers ----------

function serverError(string message, error err) returns http:InternalServerError {
    log:printError(message, err);
    return {body: {message}};
}

// ---------- REST API ----------

service /restaurants on new http:Listener(restaurantServicePort) {

    // Register a new restaurant
    resource function post registerRestaurant(@http:Payload Restaurant restaurant)
            returns http:Created|http:Conflict|http:InternalServerError {

        Restaurant?|error existing = findRestaurant(restaurant.restaurantId);
        if existing is error {
            return serverError("Failed to register restaurant", existing);
        }
        if existing is Restaurant {
            return <http:Conflict>{body: {message: "Restaurant already exists"}};
        }

        error? saved = createRestaurant(restaurant);
        if saved is error {
            return serverError("Failed to register restaurant", saved);
        }
        return <http:Created>{
            body: {message: "Restaurant registered successfully", restaurant}
        };
    }

    // List all restaurants
    resource function get list() returns http:Ok|http:InternalServerError {
        Restaurant[]|error restaurants = listRestaurants();
        if restaurants is error {
            return serverError("Failed to list restaurants", restaurants);
        }
        return <http:Ok>{body: restaurants};
    }

    // Health check
    resource function get health() returns http:Ok {
        return {body: {service: "restaurant-service", status: "UP"}};
    }

    // Get one restaurant
    resource function get [string restaurantId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        Restaurant?|error restaurant = findRestaurant(restaurantId);
        if restaurant is error {
            return serverError("Failed to retrieve restaurant", restaurant);
        }
        if restaurant is () {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }
        return <http:Ok>{body: restaurant};
    }

    // Add menu item
    resource function post [string restaurantId]/menu(@http:Payload MenuItem item)
            returns http:Created|http:NotFound|http:InternalServerError {

        Restaurant?|error restaurant = findRestaurant(restaurantId);
        if restaurant is error {
            return serverError("Failed to add menu item", restaurant);
        }
        if restaurant is () {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }

        error? saved = addMenuItem(restaurantId, item);
        if saved is error {
            return serverError("Failed to add menu item", saved);
        }
        return <http:Created>{
            body: {message: "Menu item added successfully", restaurantId, item}
        };
    }

    // Update menu item
    resource function put [string restaurantId]/menu/[string itemId](@http:Payload MenuItem item)
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error updated = updateMenuItem(restaurantId, itemId, item);
        if updated is error {
            return serverError("Failed to update menu item", updated);
        }
        if !updated {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:Ok>{
            body: {message: "Menu item updated successfully", restaurantId, itemId}
        };
    }

    // Remove menu item
    resource function delete [string restaurantId]/menu/[string itemId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error removed = removeMenuItem(restaurantId, itemId);
        if removed is error {
            return serverError("Failed to remove menu item", removed);
        }
        if !removed {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:Ok>{
            body: {message: "Menu item removed successfully", restaurantId, itemId}
        };
    }

    // Get restaurant menu
    resource function get [string restaurantId]/menu()
            returns http:Ok|http:InternalServerError {
        MenuItem[]|error menu = getMenu(restaurantId);
        if menu is error {
            return serverError("Failed to retrieve menu", menu);
        }
        return <http:Ok>{body: menu};
    }

    // Set opening hours
    resource function put [string restaurantId]/hours(@http:Payload OpeningHours hours)
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error updated = setOpeningHours(restaurantId, hours);
        if updated is error {
            return serverError("Failed to update opening hours", updated);
        }
        if !updated {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }
        return <http:Ok>{
            body: {message: "Opening hours updated successfully", restaurantId, hours}
        };
    }

    // Is the restaurant open right now?
    resource function get [string restaurantId]/is\-open()
            returns http:Ok|http:InternalServerError {
        boolean|error open = isOpenNow(restaurantId);
        if open is error {
            return serverError("Failed to check opening status", open);
        }
        return <http:Ok>{body: {restaurantId, isOpen: open}};
    }

    // Check stock for one item
    resource function get [string restaurantId]/stock/[string itemId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        int?|error stock = checkStock(restaurantId, itemId);
        if stock is error {
            return serverError("Failed to check stock", stock);
        }
        if stock is () {
            return <http:NotFound>{body: {message: "Item not found"}};
        }
        return <http:Ok>{body: {restaurantId, itemId, stock}};
    }

    // Reserve stock
    resource function post [string restaurantId]/stock/reserve(@http:Payload StockRequest request)
            returns http:Ok|http:Conflict|http:BadRequest|http:InternalServerError {
        boolean|error reserved = reserveStock(restaurantId, request.items);
        if reserved is error {
            return <http:BadRequest>{body: {message: reserved.message()}};
        }
        if !reserved {
            return <http:Conflict>{body: {message: "Insufficient stock"}};
        }
        return <http:Ok>{body: {message: "Stock reserved successfully", restaurantId}};
    }

    // Release stock
    resource function post [string restaurantId]/stock/release(@http:Payload StockRequest request)
            returns http:Ok|http:InternalServerError {
        error? released = releaseStock(restaurantId, request.items);
        if released is error {
            return serverError("Failed to release stock", released);
        }
        return <http:Ok>{body: {message: "Stock released successfully", restaurantId}};
    }
}