# Database initialisation

Each of the 7 services owns its own MongoDB database. The `*-init.js` scripts
create that database's collections and indexes. They are safe to run more than once.

## How to run

mongosh "mongodb://localhost:27017" --file database/<service>-init.js

## Design

| Database | Collections | Indexes |
|---|---|---|
| customer_db | customers, addresses, order_history, processed_events | customers.email unique; addresses.customerId; order_history.customerId |
| restaurant_db | restaurants, menu_items, inventory, opening_hours, processed_events | menu_items.restaurantId; inventory.menuItemId unique; opening_hours.restaurantId unique |
| order_db | orders, processed_events | orders.customerId; orders.status |
| payment_db | payments, processed_events | payments.orderId unique |
| delivery_db | drivers, deliveries, processed_events | deliveries.orderId unique; drivers.available |
| notification_db | notifications, processed_events | notifications.recipientId |
| admin_db | restaurant_stats, delivery_stats, processed_events | restaurant_stats.restaurantId unique |

Every `processed_events` collection has a unique index on `eventId`, so a Kafka
event delivered twice is only handled once (idempotency).

## Ownership

- Lavinia: customer, restaurant, order, payment
- Manfred: delivery, notification, admin