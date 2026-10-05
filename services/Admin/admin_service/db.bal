import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({connection: mongoUri});
final mongodb:Database db = check mongoClient->getDatabase(dbName);

// Idempotency: returns true the first time an eventId is seen, false for duplicates
function markProcessed(string eventId) returns boolean|error {
    mongodb:Collection c = check db->getCollection("processed_events");
    int n = check c->countDocuments({eventId: eventId});
    if n > 0 {
        return false;
    }
    check c->insertOne({eventId: eventId, processedAt: nowIso()});
    return true;
}

function ensureSummary(string orderId) returns error? {
    mongodb:Collection c = check db->getCollection("order_summaries");
    int n = check c->countDocuments({orderId: orderId});
    if n == 0 {
        check c->insertOne({orderId: orderId});
    }
}

function updateSummary(string orderId, map<json> fields) returns error? {
    mongodb:Collection c = check db->getCollection("order_summaries");
    _ = check c->updateOne({orderId: orderId}, {set: fields});
}

function listSummaries() returns OrderSummary[]|error {
    mongodb:Collection c = check db->getCollection("order_summaries");
    stream<OrderSummary, error?> s = check c->find({}, projection = {"_id": 0}, targetType = OrderSummary);
    return from OrderSummary o in s select o;
}
