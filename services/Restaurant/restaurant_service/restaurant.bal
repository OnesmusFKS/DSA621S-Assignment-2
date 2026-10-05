import ballerina/log;
import ballerina/time;

public function main() {
    log:printInfo("Restaurant service running on port " + port.toString());
}

// ---------- opening hours ----------
function toMinutes(string hhmm) returns int|error {
    int? colon = hhmm.indexOf(":");
    if colon is () {
        return error("Invalid time '" + hhmm + "', expected HH:mm");
    }
    int hours = check int:fromString(hhmm.substring(0, colon));
    int minutes = check int:fromString(hhmm.substring(colon + 1));
    if hours < 0 || hours > 23 || minutes < 0 || minutes > 59 {
        return error("Time out of range: " + hhmm);
    }
    return hours * 60 + minutes;
}

function isOpenNow(string restaurantId) returns boolean|error {
    Restaurant? restaurant = check findRestaurant(restaurantId);
    if restaurant is () {
        return error RestaurantNotFoundError("Restaurant not found: " + restaurantId);
    }
    OpeningHours? hours = restaurant?.hours;
    if hours is () {
        return false; // no hours set means we treat it as closed
    }

    time:Civil now = time:utcToCivil(time:utcNow());
    int current = now.hour * 60 + now.minute;
    int openAt = check toMinutes(hours.openTime);
    int closeAt = check toMinutes(hours.closeTime);

    if openAt <= closeAt {
        return current >= openAt && current < closeAt;
    }
    // Overnight hours, e.g. 18:00 -> 02:00
    return current >= openAt || current < closeAt;
}

// ---------- stock ----------
// Each decrement only succeeds if enough stock is left (atomic per item).
// If any item fails, everything reserved so far is put back.
function reserveStock(string restaurantId, OrderItem[] items) returns boolean|error {
    OrderItem[] reserved = [];

    foreach OrderItem item in items {
        if item.quantity <= 0 {
            check rollback(restaurantId, reserved);
            return error InvalidQuantityError("Invalid quantity for item " + item.itemId);
        }
        boolean ok = check decrementStockAtomic(restaurantId, item.itemId, item.quantity);
        if !ok {
            check rollback(restaurantId, reserved);
            return false;
        }
        reserved.push(item);
    }
    return true;
}

function rollback(string restaurantId, OrderItem[] reserved) returns error? {
    if reserved.length() > 0 {
        check releaseStock(restaurantId, reserved);
    }
}

function releaseStock(string restaurantId, OrderItem[] items) returns error? {
    foreach OrderItem item in items {
        check incrementStock(restaurantId, item.itemId, item.quantity);
    }
}

// Business rejections (closed, unknown restaurant, bad quantity, not enough stock) become
// reserved = false with a reason. Infrastructure errors are returned so the event is retried.
function processOrder(OrderCreated created) returns StockDecision|error {
    boolean|error open = isOpenNow(created.restaurantId);
    if open is RestaurantNotFoundError {
        return {reserved: false, reason: open.message()};
    }
    if open is error {
        return open;
    }
    if !open {
        return {reserved: false, reason: "restaurant is closed"};
    }

    boolean|error reserved = reserveStock(created.restaurantId, created.items);
    if reserved is InvalidQuantityError {
        return {reserved: false, reason: reserved.message()};
    }
    if reserved is error {
        return reserved;
    }
    if reserved {
        return {reserved: true};
    }
    return {reserved: false, reason: "insufficient stock"};
}
