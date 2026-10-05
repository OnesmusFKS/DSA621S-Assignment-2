// NOTE: Ballerina packages can't share code, so shared shapes are duplicated per service.

const string PENDING = "PENDING";
const string COMPLETED = "COMPLETED";
const string FAILED = "FAILED";
const string REFUNDED = "REFUNDED";

public type PaymentInput record {|
    string orderId;
    string customerId = "";
    decimal amount;
    string method = "CARD";
|};

public type Payment record {|
    string paymentId;
    string orderId;
    string customerId = "";
    decimal amount;
    string method = "CARD";
    string status;
    string reason = "";
    string createdAt;
    string updatedAt;
|};
