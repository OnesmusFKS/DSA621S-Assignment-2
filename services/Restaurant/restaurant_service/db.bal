import ballerinax/mongodb;

// Database isolation: this service owns ONLY restaurant_db.
final mongodb:Client mongoClient = check new ({connection: mongoUri});
final mongodb:Database db = check mongoClient->getDatabase(dbName);

function getCollection(string name) returns mongodb:Collection|error => db->getCollection(name);

// ---------- reliability ----------
function isProcessed(string eventId) returns boolean|error {
    mongodb:Collection c = check getCollection("processed_events");
    int n = check c->countDocuments({eventId: eventId});
    return n > 0;
}

// Call AFTER the event was handled successfully
function markProcessed(string eventId) returns error? {
    mongodb:Collection c = check getCollection("processed_events");
    check c->insertOne({eventId: eventId, processedAt: nowIso()});
}

// Used by /health
function checkDbConnection() returns boolean {
    string[]|error names = db->listCollectionNames();
    return names is string[];
}

// ---------- restaurants ----------
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

// ---------- menu ----------
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

// ---------- stock ----------
function checkStock(string restaurantId, string itemId) returns int?|error {
    mongodb:Collection menu = check getCollection("menu_items");
    MenuItem? item = check menu->findOne(
        {restaurantId, itemId},
        projection = {"_id": 0},
        targetType = MenuItem
    );
    return item is () ? () : item.stock;
}

// Conditional decrement: only succeeds if enough stock is left, so two orders can't oversell
function decrementStockAtomic(string restaurantId, string itemId, int qty) returns boolean|error {
    mongodb:Collection menu = check getCollection("menu_items");
    mongodb:UpdateResult res = check menu->updateOne(
        {restaurantId, itemId, stock: {"$gte": qty}},
        {inc: {stock: -qty}}
    );
    return res.modifiedCount > 0;
}

// Put stock back (cancel / failure / rollback)
function incrementStock(string restaurantId, string itemId, int qty) returns error? {
    mongodb:Collection menu = check getCollection("menu_items");
    _ = check menu->updateOne({restaurantId, itemId}, {inc: {stock: qty}});
}

// ---------- reservations ----------
function insertReservation(StockReservation r) returns error? {
    mongodb:Collection c = check getCollection("stock_reservations");
    check c->insertOne(r);
}

function findReservation(string orderId) returns StockReservation|error? {
    mongodb:Collection c = check getCollection("stock_reservations");
    return check c->findOne({orderId: orderId}, projection = {"_id": 0}, targetType = StockReservation);
}

function markReservationReleased(string orderId) returns error? {
    mongodb:Collection c = check getCollection("stock_reservations");
    _ = check c->updateOne({orderId: orderId}, {set: {released: true}});
}
