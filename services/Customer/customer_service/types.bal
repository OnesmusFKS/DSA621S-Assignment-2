public type CustomerInput record {|
    string name;
    string email;
    string phone;
|};

public type CustomerUpdate record {|
    string name;
    string phone;
|};

public type Customer record {|
    string customerId;
    string name;
    string email;
    string phone;
    string createdAt;
|};

public type AddressInput record {|
    string label;
    string street;
    string city;
    string notes = "";
|};

public type Address record {|
    string addressId;
    string customerId;
    string label;
    string street;
    string city;
    string notes = "";
|};

public type OrderHistoryEntry record {|
    string orderId;
    string customerId;
    string restaurantId = "";
    float totalAmount = 0.0;
    string status;
    string createdAt;
    string updatedAt;
|};
