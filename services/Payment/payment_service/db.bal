import ballerinax/mongodb;

// Database isolation: this service owns ONLY payment_db.
final mongodb:Client mongoClient = check new ({connection: mongoUri});
final mongodb:Database db = check mongoClient->getDatabase(dbName);

// ---------- reliability ----------
function isProcessed(string eventId) returns boolean|error {
    mongodb:Collection c = check db->getCollection("processed_events");
    int n = check c->countDocuments({eventId: eventId});
    return n > 0;
}

// Call AFTER the event was handled successfully
function markProcessed(string eventId) returns error? {
    mongodb:Collection c = check db->getCollection("processed_events");
    check c->insertOne({eventId: eventId, processedAt: nowIso()});
}

// Used by /health
function checkDbConnection() returns boolean {
    string[]|error names = db->listCollectionNames();
    return names is string[];
}

// ---------- payments ----------
function insertPayment(Payment p) returns error? {
    mongodb:Collection c = check db->getCollection("payments");
    check c->insertOne(p);
}

function findPaymentById(string paymentId) returns Payment|error? {
    mongodb:Collection c = check db->getCollection("payments");
    return check c->findOne({paymentId: paymentId}, projection = {"_id": 0}, targetType = Payment);
}

function findPaymentByOrder(string orderId) returns Payment|error? {
    mongodb:Collection c = check db->getCollection("payments");
    return check c->findOne({orderId: orderId}, projection = {"_id": 0}, targetType = Payment);
}

// Idempotency check: one payment per order
function paymentExistsForOrder(string orderId) returns boolean|error {
    mongodb:Collection c = check db->getCollection("payments");
    int n = check c->countDocuments({orderId: orderId});
    return n > 0;
}

function updatePaymentStatus(string paymentId, string status, string reason) returns error? {
    mongodb:Collection c = check db->getCollection("payments");
    _ = check c->updateOne({paymentId: paymentId}, {set: {status: status, reason: reason, updatedAt: nowIso()}});
}
