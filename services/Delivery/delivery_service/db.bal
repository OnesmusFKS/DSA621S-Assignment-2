import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({connection: mongoUri});

function getDb() returns mongodb:Database|error {
    return mongoClient->getDatabase(dbName);
}

// Idempotency: returns true the first time an eventId is seen, false for duplicates
function markProcessed(string eventId) returns boolean|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("processed_events");
    int n = check c->countDocuments({"eventId": eventId});
    if n > 0 {
        return false;
    }
    check c->insertOne({"eventId": eventId, "processedAt": nowIso()});
    return true;
}

// ---------- drivers ----------
function insertDriver(Driver d) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    check c->insertOne(d);
}

function findDriver(string driverId) returns Driver|error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    return check c->findOne({"driverId": driverId}, projection = {"_id": 0}, targetType = Driver);
}

function listDrivers() returns Driver[]|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    stream<Driver, error?> s = check c->find({}, projection = {"_id": 0}, targetType = Driver);
    return from Driver d in s select d;
}

function setAvailability(string driverId, boolean available) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    _ = check c->updateOne({"driverId": driverId}, {set: {"available": available, "updatedAt": nowIso()}});
}

function setLocation(string driverId, float lat, float lng) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    _ = check c->updateOne({"driverId": driverId}, {set: {"lat": lat, "lng": lng, "updatedAt": nowIso()}});
}

// Atomically claim one available driver (only one caller can flip available -> false)
function claimDriver() returns Driver|error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("drivers");
    stream<Driver, error?> s = check c->find({"available": true}, projection = {"_id": 0}, targetType = Driver);
    Driver[] free = check from Driver d in s select d;
    foreach Driver d in free {
        mongodb:UpdateResult r = check c->updateOne({"driverId": d.driverId, "available": true}, {set: {"available": false}});
        if r.modifiedCount == 1 {
            return d;
        }
    }
    return ();
}

// ---------- deliveries ----------
function insertDelivery(Delivery d) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("deliveries");
    check c->insertOne(d);
}

function findDelivery(string orderId) returns Delivery|error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("deliveries");
    return check c->findOne({"orderId": orderId}, projection = {"_id": 0}, targetType = Delivery);
}

function listDeliveries(string status) returns Delivery[]|error {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("deliveries");
    map<json> filter = status == "" ? {} : {"status": status};
    stream<Delivery, error?> s = check c->find(filter, projection = {"_id": 0}, targetType = Delivery);
    return from Delivery d in s select d;
}

function updateDelivery(string orderId, map<json> fields) returns error? {
    mongodb:Database db = check getDb();
    mongodb:Collection c = check db->getCollection("deliveries");
    fields["updatedAt"] = nowIso();
    _ = check c->updateOne({"orderId": orderId}, {set: fields});
}
