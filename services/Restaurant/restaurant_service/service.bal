import ballerina/http;
import ballerina/log;

// ---------- REST helpers ----------

function serverError(string message, error err) returns http:InternalServerError {
    log:printError(message, err);
    return {body: {message}};
}

// ---------- REST API ----------

service /restaurants on new http:Listener(port) {

    // Register a new restaurant
    resource function post registerRestaurant(@http:Payload Restaurant restaurant)
            returns http:Created|http:Conflict|http:InternalServerError {

        Restaurant?|error existing = findRestaurant(restaurant.restaurantId);
        if existing is error {
            return serverError("Failed to register restaurant", existing);
        }
        if existing is Restaurant {
            return <http:Conflict>{body: {message: "Restaurant already exists"}};
        }

        error? saved = createRestaurant(restaurant);
        if saved is error {
            return serverError("Failed to register restaurant", saved);
        }
        return <http:Created>{
            body: {message: "Restaurant registered successfully", restaurant}
        };
    }

    // List all restaurants
    resource function get list() returns http:Ok|http:InternalServerError {
        Restaurant[]|error restaurants = listRestaurants();
        if restaurants is error {
            return serverError("Failed to list restaurants", restaurants);
        }
        return <http:Ok>{body: restaurants};
    }

    // Health check
    resource function get health() returns http:Ok|http:ServiceUnavailable {
        if !checkDbConnection() {
            return <http:ServiceUnavailable>{body: {"service": "restaurant-service", "status": "DOWN"}};
        }
        return {body: {"service": "restaurant-service", "status": "UP"}};
    }

    // Get one restaurant
    resource function get [string restaurantId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        Restaurant?|error restaurant = findRestaurant(restaurantId);
        if restaurant is error {
            return serverError("Failed to retrieve restaurant", restaurant);
        }
        if restaurant is () {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }
        return <http:Ok>{body: restaurant};
    }

    // Add menu item
    resource function post [string restaurantId]/menu(@http:Payload MenuItem item)
            returns http:Created|http:NotFound|http:InternalServerError {

        Restaurant?|error restaurant = findRestaurant(restaurantId);
        if restaurant is error {
            return serverError("Failed to add menu item", restaurant);
        }
        if restaurant is () {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }

        error? saved = addMenuItem(restaurantId, item);
        if saved is error {
            return serverError("Failed to add menu item", saved);
        }
        return <http:Created>{
            body: {message: "Menu item added successfully", restaurantId, item}
        };
    }

    // Update menu item
    resource function put [string restaurantId]/menu/[string itemId](@http:Payload MenuItem item)
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error updated = updateMenuItem(restaurantId, itemId, item);
        if updated is error {
            return serverError("Failed to update menu item", updated);
        }
        if !updated {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:Ok>{
            body: {message: "Menu item updated successfully", restaurantId, itemId}
        };
    }

    // Remove menu item
    resource function delete [string restaurantId]/menu/[string itemId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error removed = removeMenuItem(restaurantId, itemId);
        if removed is error {
            return serverError("Failed to remove menu item", removed);
        }
        if !removed {
            return <http:NotFound>{body: {message: "Menu item not found"}};
        }
        return <http:Ok>{
            body: {message: "Menu item removed successfully", restaurantId, itemId}
        };
    }

    // Get restaurant menu
    resource function get [string restaurantId]/menu()
            returns http:Ok|http:InternalServerError {
        MenuItem[]|error menu = getMenu(restaurantId);
        if menu is error {
            return serverError("Failed to retrieve menu", menu);
        }
        return <http:Ok>{body: menu};
    }

    // Set opening hours
    resource function put [string restaurantId]/hours(@http:Payload OpeningHours hours)
            returns http:Ok|http:NotFound|http:InternalServerError {
        boolean|error updated = setOpeningHours(restaurantId, hours);
        if updated is error {
            return serverError("Failed to update opening hours", updated);
        }
        if !updated {
            return <http:NotFound>{body: {message: "Restaurant not found"}};
        }
        return <http:Ok>{
            body: {message: "Opening hours updated successfully", restaurantId, hours}
        };
    }

    // Is the restaurant open right now?
    resource function get [string restaurantId]/is\-open()
            returns http:Ok|http:InternalServerError {
        boolean|error open = isOpenNow(restaurantId);
        if open is error {
            return serverError("Failed to check opening status", open);
        }
        return <http:Ok>{body: {restaurantId, isOpen: open}};
    }

    // Check stock for one item
    resource function get [string restaurantId]/stock/[string itemId]()
            returns http:Ok|http:NotFound|http:InternalServerError {
        int?|error stock = checkStock(restaurantId, itemId);
        if stock is error {
            return serverError("Failed to check stock", stock);
        }
        if stock is () {
            return <http:NotFound>{body: {message: "Item not found"}};
        }
        return <http:Ok>{body: {restaurantId, itemId, stock}};
    }

    // Reserve stock
    resource function post [string restaurantId]/stock/reserve(@http:Payload StockRequest request)
            returns http:Ok|http:Conflict|http:BadRequest|http:InternalServerError {
        boolean|error reserved = reserveStock(restaurantId, request.items);
        if reserved is error {
            return <http:BadRequest>{body: {message: reserved.message()}};
        }
        if !reserved {
            return <http:Conflict>{body: {message: "Insufficient stock"}};
        }
        return <http:Ok>{body: {message: "Stock reserved successfully", restaurantId}};
    }

    // Release stock
    resource function post [string restaurantId]/stock/release(@http:Payload StockRequest request)
            returns http:Ok|http:InternalServerError {
        error? released = releaseStock(restaurantId, request.items);
        if released is error {
            return serverError("Failed to release stock", released);
        }
        return <http:Ok>{body: {message: "Stock released successfully", restaurantId}};
    }
}