// NOTE: Ballerina packages can't share code, so shared shapes are duplicated per service.

// ---------- statuses ----------
const string CREATED = "CREATED";
const string CONFIRMED = "CONFIRMED";
const string PREPARING = "PREPARING";
const string READY = "READY";
const string OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY";
const string DELIVERED = "DELIVERED";
const string CANCELLED = "CANCELLED";

final readonly & map<string[]> allowedTransitions = {
    [CREATED]: [CONFIRMED, CANCELLED],
    [CONFIRMED]: [PREPARING, CANCELLED],
    [PREPARING]: [READY],
    [READY]: [OUT_FOR_DELIVERY],
    [OUT_FOR_DELIVERY]: [DELIVERED],
    [DELIVERED]: [],
    [CANCELLED]: []
};

// ---------- records ----------
public type OrderItem record {|
    string itemId;
    string name?;
    int quantity;
    decimal price;
|};

public type CreateOrderRequest record {|
    string customerId;
    string restaurantId;
    OrderItem[] items;
|};

public type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    string createdAt;
    string updatedAt;
    string driverId?;
    string idempotencyKey?;
|};

// Audit trail: one entry per status transition
public type StatusHistoryEntry record {|
    string orderId;
    string fromStatus;
    string toStatus;
    string reason = "";
    string actor;
    string timestamp;
|};

// ---------- errors ----------
public type OrderNotFoundError distinct error;
public type InvalidTransitionError distinct error;
public type ConcurrentUpdateError distinct error;
