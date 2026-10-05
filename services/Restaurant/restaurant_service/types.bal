// NOTE: Ballerina packages can't share code, so shared shapes are duplicated per service.

public type OpeningHours record {|
    string openTime;   // "HH:mm", UTC
    string closeTime;  // "HH:mm", UTC (may be earlier than openTime for overnight hours)
|};

public type Restaurant record {
    string restaurantId;
    string name;
    string address?;
    OpeningHours hours?;
};

public type MenuItem record {
    string itemId;
    string restaurantId?;
    string name;
    decimal price;
    int stock = 0;
};

public type OrderItem record {
    string itemId;
    int quantity;
};

public type StockRequest record {
    OrderItem[] items;
};

// Payload consumed from orders.created
public type OrderCreated record {
    string orderId;
    string restaurantId;
    OrderItem[] items;
};

// Outcome of checking opening hours + stock for one order
public type StockDecision record {|
    boolean reserved;
    string reason = "";
|};

// One document per order we have decided on. Used for idempotency (duplicate
// events never reserve twice) and to give the stock back when the order is cancelled.
public type StockReservation record {|
    string orderId;
    string restaurantId;
    OrderItem[] items;
    boolean reserved;
    string reason = "";
    boolean released = false;
    string createdAt;
|};

public type RestaurantNotFoundError distinct error;
public type InvalidQuantityError distinct error;
