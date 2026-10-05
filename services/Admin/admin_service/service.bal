import ballerina/http;
import ballerina/time;

service /admin on new http:Listener(port) {

    // Overall platform numbers
    resource function get reports/summary() returns SummaryReport|error {
        OrderSummary[] all = check listSummaries();
        map<int> byStatus = {};
        int delivered = 0;
        int cancelled = 0;
        float revenue = 0.0;
        foreach OrderSummary o in all {
            byStatus[o.status] = (byStatus[o.status] ?: 0) + 1;
            if o.status == "DELIVERED" {
                delivered += 1;
            } else if o.status == "CANCELLED" {
                cancelled += 1;
            }
            if o.paid && o.status != "CANCELLED" {
                revenue += o.totalAmount;
            }
        }
        return {
            totalOrders: all.length(),
            delivered: delivered,
            cancelled: cancelled,
            inProgress: all.length() - delivered - cancelled,
            totalRevenue: revenue,
            ordersByStatus: byStatus
        };
    }

    // Statistics for every restaurant
    resource function get reports/restaurants() returns RestaurantStats[]|error {
        OrderSummary[] all = check listSummaries();
        return buildRestaurantStats(all);
    }

    // Statistics for one restaurant
    resource function get reports/restaurants/[string restaurantId]() returns RestaurantStats|http:NotFound|error {
        OrderSummary[] all = check listSummaries();
        foreach RestaurantStats s in buildRestaurantStats(all) {
            if s.restaurantId == restaurantId {
                return s;
            }
        }
        return <http:NotFound>{body: {message: "No orders recorded for this restaurant"}};
    }

    // Delivery performance
    resource function get reports/deliveries() returns DeliveryReport|error {
        OrderSummary[] all = check listSummaries();
        int total = 0;
        int completed = 0;
        float totalSeconds = 0.0;
        int timed = 0;
        map<DriverStats> drivers = {};

        foreach OrderSummary o in all {
            if o.assignedAt == "" {
                continue;
            }
            total += 1;
            boolean done = o.deliveredAt != "";
            if done {
                completed += 1;
                time:Utc a = check time:utcFromString(o.assignedAt);
                time:Utc b = check time:utcFromString(o.deliveredAt);
                totalSeconds += <float>time:utcDiffSeconds(b, a);
                timed += 1;
            }
            if o.driverId != "" {
                DriverStats ds = drivers[o.driverId] ?: {driverId: o.driverId, assigned: 0, completed: 0};
                ds.assigned += 1;
                if done {
                    ds.completed += 1;
                }
                drivers[o.driverId] = ds;
            }
        }
        return {
            totalDeliveries: total,
            completed: completed,
            inProgress: total - completed,
            avgDeliveryMinutes: timed == 0 ? 0.0 : totalSeconds / <float>timed / 60.0,
            drivers: drivers.toArray()
        };
    }
}

function buildRestaurantStats(OrderSummary[] all) returns RestaurantStats[] {
    map<RestaurantStats> m = {};
    foreach OrderSummary o in all {
        if o.restaurantId == "" {
            continue;
        }
        RestaurantStats s = m[o.restaurantId] ?: {
            restaurantId: o.restaurantId,
            totalOrders: 0,
            delivered: 0,
            cancelled: 0,
            revenue: 0.0,
            avgOrderValue: 0.0
        };
        s.totalOrders += 1;
        if o.status == "DELIVERED" {
            s.delivered += 1;
        } else if o.status == "CANCELLED" {
            s.cancelled += 1;
        }
        if o.paid && o.status != "CANCELLED" {
            s.revenue += o.totalAmount;
        }
        m[o.restaurantId] = s;
    }
    RestaurantStats[] out = m.toArray();
    foreach RestaurantStats s in out {
        s.avgOrderValue = s.totalOrders == 0 ? 0.0 : s.revenue / <float>s.totalOrders;
    }
    return out;
}
