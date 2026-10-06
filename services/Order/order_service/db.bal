import ballerinax/mongodb;

// Database isolation: this service owns ONLY order_db.
final mongodb:Client mongoClient = check new ({connection: mongoUri});

function getDb() returns mongodb:Database|error {
    return mongoClient->getDatabase(dbName);
}

// ---------- reliability ----------
function isProcessed(string eventId) returns boolean|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("processed_events");
    int n = check c->countDocuments({"eventId": eventId});
    return n > 0;
}

// Call AFTER the event was handled successfully
function markProcessed(string eventId) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("processed_events");
    check c->insertOne({"eventId": eventId, "processedAt": nowIso()});
}

// Used by /health
function checkDbConnection() returns boolean {
    mongodb:Database db = checkpanic getDb();
    string[]|error names = db->listCollectionNames();
    return names is string[];
}

// ---------- orders ----------
function insertOrder(Order o) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    check c->insertOne(o);
}

function findOrderById(string orderId) returns Order|error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    return check c->findOne({"orderId": orderId}, projection = {"_id": 0}, targetType = Order);
}

// Avoids duplicate orders when a client retries the same request
function findOrderByIdempotencyKey(string key) returns Order|error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    return check c->findOne({"idempotencyKey": key}, projection = {"_id": 0}, targetType = Order);
}

function listOrders(map<json> filter) returns Order[]|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    stream<Order, error?> s = check c->find(filter, projection = {"_id": 0}, targetType = Order);
    return from Order o in s select o;
}

function findOrdersByCustomer(string customerId) returns Order[]|error => listOrders({"customerId": customerId});

function findOrdersByRestaurant(string restaurantId) returns Order[]|error => listOrders({"restaurantId": restaurantId});

// Compare-and-set: only flips the status if it is still `fromStatus`. Returns false if someone else changed it first.
function updateOrderStatusDoc(string orderId, string fromStatus, string toStatus, string updatedAt) returns boolean|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    mongodb:UpdateResult r = check c->updateOne(
        {"orderId": orderId, "status": fromStatus},
        {set: {"status": toStatus, "updatedAt": updatedAt}}
    );
    return r.matchedCount > 0;
}

function setOrderDriver(string orderId, string driverId) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("orders");
    _ = check c->updateOne({"orderId": orderId}, {set: {"driverId": driverId, "updatedAt": nowIso()}});
}

// ---------- status history ----------
function appendStatusHistory(StatusHistoryEntry e) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("order_status_history");
    check c->insertOne(e);
}

function listStatusHistory(string orderId) returns StatusHistoryEntry[]|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("order_status_history");
    stream<StatusHistoryEntry, error?> s = check c->find({"orderId": orderId}, projection = {"_id": 0}, targetType = StatusHistoryEntry);
    return from StatusHistoryEntry h in s select h;
}
