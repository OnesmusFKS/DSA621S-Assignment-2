public type OrderSummary record {|
    string orderId;
    string restaurantId = "";
    string customerId = "";
    float totalAmount = 0.0;
    string status = "CREATED";
    boolean paid = false;
    string driverId = "";
    string createdAt = "";
    string assignedAt = "";
    string deliveredAt = "";
|};

public type SummaryReport record {|
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    float totalRevenue;
    map<int> ordersByStatus;
|};

public type RestaurantStats record {|
    string restaurantId;
    int totalOrders;
    int delivered;
    int cancelled;
    float revenue;
    float avgOrderValue;
|};

public type DriverStats record {|
    string driverId;
    int assigned;
    int completed;
|};

public type DeliveryReport record {|
    int totalDeliveries;
    int completed;
    int inProgress;
    float avgDeliveryMinutes;
    DriverStats[] drivers;
|};
