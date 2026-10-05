import ballerina/http;
import ballerina/uuid;

public type DriverCreated record {|
    *http:Created;
    Driver body;
|};

listener http:Listener httpListener = new (port);

service /drivers on httpListener {

    resource function post .(DriverInput input) returns DriverCreated|error {
        Driver d = {
            driverId: uuid:createType4AsString(),
            name: input.name,
            phone: input.phone,
            vehicle: input.vehicle,
            updatedAt: nowIso()
        };
        check insertDriver(d);
        // a new free driver can pick up waiting orders
        check assignPending();
        return <DriverCreated>{body: d};
    }

    resource function get .() returns Driver[]|error {
        return listDrivers();
    }

    resource function get [string driverId]() returns Driver|http:NotFound|error {
        Driver? d = check findDriver(driverId);
        if d is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        return d;
    }

    resource function put [string driverId]/availability(AvailabilityUpdate u) returns Driver|http:NotFound|error {
        Driver? d = check findDriver(driverId);
        if d is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        check setAvailability(driverId, u.available);
        d.available = u.available;
        if u.available {
            check assignPending();
        }
        return d;
    }

    // Driver location simulation / real-time tracking
    resource function put [string driverId]/location(LocationUpdate u) returns Driver|http:NotFound|error {
        Driver? d = check findDriver(driverId);
        if d is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        check setLocation(driverId, u.lat, u.lng);
        d.lat = u.lat;
        d.lng = u.lng;
        return d;
    }
}

service /deliveries on httpListener {

    resource function get .(string status = "") returns Delivery[]|error {
        return listDeliveries(status);
    }

    resource function get [string orderId]() returns Delivery|http:NotFound|error {
        Delivery? d = check findDelivery(orderId);
        if d is () {
            return <http:NotFound>{body: {message: "No delivery for this order"}};
        }
        return d;
    }

    // Where is my driver right now?
    resource function get [string orderId]/location() returns DriverLocation|http:NotFound|error {
        Delivery? d = check findDelivery(orderId);
        if d is () || d.driverId == "" {
            return <http:NotFound>{body: {message: "No driver assigned yet"}};
        }
        Driver? drv = check findDriver(d.driverId);
        if drv is () {
            return <http:NotFound>{body: {message: "Driver not found"}};
        }
        return {driverId: drv.driverId, lat: drv.lat, lng: drv.lng, updatedAt: drv.updatedAt};
    }

    // Driver collected the food
    resource function post [string orderId]/pickup() returns Delivery|http:NotFound|http:Conflict|error {
        Delivery? d = check findDelivery(orderId);
        if d is () {
            return <http:NotFound>{body: {message: "No delivery for this order"}};
        }
        if d.status != ASSIGNED {
            return <http:Conflict>{body: {message: "Delivery must be ASSIGNED, it is " + d.status}};
        }
        check updateDelivery(orderId, {status: PICKED_UP});
        check publishEvent("delivery.pickedup", orderId, {
            eventId: uuid:createType4AsString(),
            orderId: orderId,
            driverId: d.driverId,
            timestamp: nowIso()
        });
        d.status = PICKED_UP;
        return d;
    }

    // Driver handed the food to the customer
    resource function post [string orderId]/complete() returns Delivery|http:NotFound|http:Conflict|error {
        Delivery? d = check findDelivery(orderId);
        if d is () {
            return <http:NotFound>{body: {message: "No delivery for this order"}};
        }
        if d.status != PICKED_UP {
            return <http:Conflict>{body: {message: "Delivery must be PICKED_UP, it is " + d.status}};
        }
        check updateDelivery(orderId, {status: DELIVERED});
        check setAvailability(d.driverId, true);
        check publishEvent("delivery.completed", orderId, {
            eventId: uuid:createType4AsString(),
            orderId: orderId,
            customerId: d.customerId,
            driverId: d.driverId,
            timestamp: nowIso()
        });
        d.status = DELIVERED;
        check assignPending();
        return d;
    }
}
