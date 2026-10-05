import ballerina/http;
import ballerina/uuid;

public type CustomerCreated record {|
    *http:Created;
    Customer body;
|};

public type AddressCreated record {|
    *http:Created;
    Address body;
|};

service /customers on new http:Listener(port) {

    // Register a customer
    resource function post .(CustomerInput input) returns CustomerCreated|http:BadRequest|http:Conflict|error {
        if input.name.trim() == "" || input.email.trim() == "" {
            return <http:BadRequest>{body: {message: "name and email are required"}};
        }
        Customer? existing = check findCustomerByEmail(input.email);
        if existing is Customer {
            return <http:Conflict>{body: {message: "Email already registered"}};
        }
        Customer c = {
            customerId: uuid:createType4AsString(),
            name: input.name,
            email: input.email,
            phone: input.phone,
            createdAt: nowIso()
        };
        check insertCustomer(c);
        return <CustomerCreated>{body: c};
    }

    resource function get [string customerId]() returns Customer|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        if c is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        return c;
    }

    resource function put [string customerId](CustomerUpdate update) returns Customer|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        if c is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        check updateCustomer(customerId, update);
        c.name = update.name;
        c.phone = update.phone;
        return c;
    }

    // Delivery addresses
    resource function post [string customerId]/addresses(AddressInput input) returns AddressCreated|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        if c is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        Address a = {
            addressId: uuid:createType4AsString(),
            customerId: customerId,
            label: input.label,
            street: input.street,
            city: input.city,
            notes: input.notes
        };
        check insertAddress(a);
        return <AddressCreated>{body: a};
    }

    resource function get [string customerId]/addresses() returns Address[]|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        if c is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        return listAddresses(customerId);
    }

    resource function delete [string customerId]/addresses/[string addressId]() returns http:NoContent|http:NotFound|error {
        boolean deleted = check deleteAddress(customerId, addressId);
        if !deleted {
            return <http:NotFound>{body: {message: "Address not found"}};
        }
        return http:NO_CONTENT;
    }

    // Historical orders (filled from Kafka events)
    resource function get [string customerId]/orders() returns OrderHistoryEntry[]|http:NotFound|error {
        Customer? c = check findCustomer(customerId);
        if c is () {
            return <http:NotFound>{body: {message: "Customer not found"}};
        }
        return listOrderHistory(customerId);
    }
}
