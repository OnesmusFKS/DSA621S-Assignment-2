import ballerina/http;

public type PaymentCreated record {|
    *http:Created;
    Payment body;
|};

service /payments on new http:Listener(port) {

    // processPayment: simulate success/failure, saves the result and publishes it
    resource function post process(PaymentInput input) returns PaymentCreated|http:BadRequest|http:Conflict|error {
        if input.orderId.trim() == "" || input.amount <= 0d {
            return <http:BadRequest>{body: {message: "orderId and a positive amount are required"}};
        }
        boolean exists = check paymentExistsForOrder(input.orderId);
        if exists {
            return <http:Conflict>{body: {message: "A payment already exists for this order"}};
        }
        Payment p = check processPayment(input.orderId, input.customerId, input.amount, input.method);
        return <PaymentCreated>{body: p};
    }

    // getPaymentByOrder ('order is a Ballerina keyword, hence the quote)
    resource function get 'order/[string orderId]() returns Payment|http:NotFound|error {
        Payment? p = check findPaymentByOrder(orderId);
        if p is () {
            return <http:NotFound>{body: {message: "No payment for this order"}};
        }
        return p;
    }

    // getPayment
    resource function get [string paymentId]() returns Payment|http:NotFound|error {
        Payment? p = check findPaymentById(paymentId);
        if p is () {
            return <http:NotFound>{body: {message: "Payment not found"}};
        }
        return p;
    }

    // refundPayment: only COMPLETED payments can be refunded
    resource function post [string paymentId]/refund() returns Payment|http:NotFound|http:Conflict|error {
        Payment? p = check findPaymentById(paymentId);
        if p is () {
            return <http:NotFound>{body: {message: "Payment not found"}};
        }
        if p.status != COMPLETED {
            return <http:Conflict>{body: {message: "Only COMPLETED payments can be refunded, this one is " + p.status}};
        }
        return refundPayment(p);
    }

    // healthCheck
    resource function get health() returns json|http:ServiceUnavailable {
        if !checkDbConnection() {
            return <http:ServiceUnavailable>{body: {status: "DOWN", 'service: "payment-service"}};
        }
        return {status: "UP", 'service: "payment-service"};
    }
}
