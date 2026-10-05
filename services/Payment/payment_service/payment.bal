import ballerina/log;
import ballerina/random;
import ballerina/uuid;

public function main() {
    log:printInfo("Payment service running on port " + port.toString());
}

// Simulated processor: returns a decline reason, or () when the payment goes through
function simulateOutcome(decimal amount) returns string? {
    if amount <= 0d {
        return "invalid amount";
    }
    if <decimal>random:createDecimal() < <decimal>failureRate {
        return "payment declined by processor (simulated)";
    }
    return ();
}

// Creates the payment, simulates the outcome and announces it on Kafka.
// Returns the existing payment untouched if the order was already paid/attempted.
function processPayment(string orderId, string customerId, decimal amount, string method) returns Payment|error {
    Payment? existing = check findPaymentByOrder(orderId);
    if existing is Payment {
        return existing;
    }

    string now = nowIso();
    Payment p = {
        paymentId: uuid:createType4AsString(),
        orderId: orderId,
        customerId: customerId,
        amount: amount,
        method: method,
        status: PENDING,
        createdAt: now,
        updatedAt: now
    };
    check insertPayment(p);

    string? failure = simulateOutcome(amount);
    if failure is string {
        check updatePaymentStatus(p.paymentId, FAILED, failure);
        p.status = FAILED;
        p.reason = failure;
        check publishPaymentFailed(p);
    } else {
        check updatePaymentStatus(p.paymentId, COMPLETED, "");
        p.status = COMPLETED;
        check publishPaymentCompleted(p);
    }
    return p;
}

// Only call for COMPLETED payments (callers check)
function refundPayment(Payment p) returns Payment|error {
    check updatePaymentStatus(p.paymentId, REFUNDED, "order cancelled");
    Payment refunded = p.clone();
    refunded.status = REFUNDED;
    refunded.reason = "order cancelled";
    check publishPaymentRefunded(refunded);
    return refunded;
}
