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

function findCustomer(string customerId) returns Customer|error? {
    mongodb:Collection c = check db->getCollection("customers");
    return check c->findOne({customerId: customerId}, projection = {"_id": 0}, targetType = Customer);
}

function findCustomerByEmail(string email) returns Customer|error? {
    mongodb:Collection c = check db->getCollection("customers");
    return check c->findOne({email: email}, projection = {"_id": 0}, targetType = Customer);
}

function insertCustomer(Customer cust) returns error? {
    mongodb:Collection c = check db->getCollection("customers");
    check c->insertOne(cust);
}

function updateCustomer(string customerId, CustomerUpdate u) returns error? {
    mongodb:Collection c = check db->getCollection("customers");
    _ = check c->updateOne({customerId: customerId}, {set: {name: u.name, phone: u.phone}});
}

function insertAddress(Address a) returns error? {
    mongodb:Collection c = check db->getCollection("addresses");
    check c->insertOne(a);
}

function listAddresses(string customerId) returns Address[]|error {
    mongodb:Collection c = check db->getCollection("addresses");
    stream<Address, error?> s = check c->find({customerId: customerId}, projection = {"_id": 0}, targetType = Address);
    return from Address a in s select a;
}

function deleteAddress(string customerId, string addressId) returns boolean|error {
    mongodb:Collection c = check db->getCollection("addresses");
    mongodb:DeleteResult r = check c->deleteOne({customerId: customerId, addressId: addressId});
    return r.deletedCount > 0;
}

function listOrderHistory(string customerId) returns OrderHistoryEntry[]|error {
    mongodb:Collection c = check db->getCollection("order_history");
    stream<OrderHistoryEntry, error?> s = check c->find({customerId: customerId}, projection = {"_id": 0}, targetType = OrderHistoryEntry);
    return from OrderHistoryEntry o in s select o;
}

function findOrderEntry(string orderId) returns OrderHistoryEntry|error? {
    mongodb:Collection c = check db->getCollection("order_history");
    return check c->findOne({orderId: orderId}, projection = {"_id": 0}, targetType = OrderHistoryEntry);
}

function insertOrderEntry(OrderHistoryEntry e) returns error? {
    mongodb:Collection c = check db->getCollection("order_history");
    check c->insertOne(e);
}

function updateOrderStatus(string orderId, string status) returns error? {
    mongodb:Collection c = check db->getCollection("order_history");
    _ = check c->updateOne({orderId: orderId}, {set: {status: status, updatedAt: nowIso()}});
}
