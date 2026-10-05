const string PENDING_DRIVER = "PENDING_DRIVER";
const string ASSIGNED = "ASSIGNED";
const string PICKED_UP = "PICKED_UP";
const string DELIVERED = "DELIVERED";
const string CANCELLED = "CANCELLED";

public type DriverInput record {|
    string name;
    string phone;
    string vehicle = "motorbike";
|};

public type Driver record {|
    string driverId;
    string name;
    string phone;
    string vehicle = "motorbike";
    boolean available = true;
    float lat = 0.0;
    float lng = 0.0;
    string updatedAt = "";
|};

public type AvailabilityUpdate record {|
    boolean available;
|};

public type LocationUpdate record {|
    float lat;
    float lng;
|};

public type Delivery record {|
    string deliveryId;
    string orderId;
    string customerId = "";
    string restaurantId = "";
    string driverId = "";
    string status;
    string createdAt;
    string updatedAt;
|};

public type DriverLocation record {|
    string driverId;
    float lat;
    float lng;
    string updatedAt;
|};
