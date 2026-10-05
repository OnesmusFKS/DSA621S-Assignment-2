import ballerina/log;

public function main() {
    log:printInfo("Order service running on port " + port.toString());
}

// ---------- state machine ----------
function isValidTransition(string currentStatus, string newStatus) returns boolean {
    string[]? allowed = allowedTransitions[currentStatus];
    if allowed is () {
        return false;
    }
    return allowed.indexOf(newStatus) is int;
}

function fetchOrder(string orderId) returns Order|error {
    Order? found = check findOrderById(orderId);
    if found is () {
        return error OrderNotFoundError(string `Order ${orderId} not found`);
    }
    return found;
}

// The only function that changes status (REST and Kafka both go through here).
function updateOrderStatus(string orderId, string newStatus, string? reason = (), string actor = "system") returns error? {
    Order existing = check fetchOrder(orderId);

    // Idempotent: events can be delivered more than once.
    if existing.status == newStatus {
        return;
    }
    if !isValidTransition(existing.status, newStatus) {
        return error InvalidTransitionError(
            string `Cannot move order ${orderId} from ${existing.status} to ${newStatus}`);
    }

    string updatedAt = nowIso();
    boolean changed = check updateOrderStatusDoc(orderId, existing.status, newStatus, updatedAt);
    if !changed {
        return error ConcurrentUpdateError(
            string `Order ${orderId} was changed by another request, please retry`);
    }

    // The audit trail must not block the transition itself
    error? h = appendStatusHistory({
        orderId: orderId,
        fromStatus: existing.status,
        toStatus: newStatus,
        reason: reason ?: "",
        actor: actor,
        timestamp: updatedAt
    });
    if h is error {
        log:printError("Could not write status history", h, orderId = orderId);
    }

    Order updated = existing.clone();
    updated.status = newStatus;
    updated.updatedAt = updatedAt;
    publishStatusChanged(updated, existing.status, reason);
}
