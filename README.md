# Distributed Food Delivery Platform (DSA612S Assignment 2)

An event-driven food delivery platform built with **Ballerina**, **Kafka** and **Docker**. It is made up of 7 independent microservices, each with its own database, communicating through Kafka topics.

## File Structure

```
DSA521S-Assignment-2/                                                                       --
├── README.md                     # Project overview (this file)                              |
├── docker-compose.yml            # Kafka, MongoDB and all 7 services                         |
├── .env.example                  # Environment variable template (copy to .env)              |
├── .gitignore                                                                                |
│                                                                                             |
├── docs/                         # Design documentation                                      |
│   ├── architecture.md           # Architecture and order event flow                         |
│   ├── events.md                 # Event payload contracts per topic                         |----- Onesmus
│   └── testing.md                # Test scenarios for the live defence                       |
│                                                                                             |
├── kafka/                        # Kafka setup                                               |
│   ├── create-topics.sh          # Creates all topics (and .dlq topics)                      |
│   └── topics.md                 # Topic table: producers, consumers, keys                   |
│                                                                                           --
├── database/                     # Per-service database initialisation                     - Lavinia / Manfred
│   ├── README.md
│   ├── admin-init.js
│   ├── customer-init.js
│   ├── delivery-init.js
│   ├── notification-init.js
│   ├── order-init.js
│   ├── payment-init.js
│   └── restaurant-init.js
│
├── UI/                           # Front-end (empty for now)                              - On one yet
│
└── services/                     # Ballerina microservices
    ├── Admin/admin_service/                                                               - Octaviano
    ├── Customer/customer_service/                                                         - Octaviano
    ├── Delivery/delivery_service/                                                         - Octaviano
    ├── Notification/notification_service/                                                 - Lucia
    ├── Order/order_service/                                                               - Lucia
    ├── Payment/payment_service/                                                           -Johannes
    └── Restaurant/restaurant_service/                                                     -Johannes
```

### Structure of each service

Every service package follows the same layout (shown for `order_service`):

```
order_service/
├── Ballerina.toml        # Package metadata (Ballerina 2201.13.5)
├── Config.toml.example   # Runtime config template
├── Dockerfile            # Container build
├── .devcontainer.json
├── .gitignore
├── order.bal             # Entry point (main)
├── service.bal           # HTTP resource functions / API
├── types.bal             # Record types and event payloads
├── config.bal            # Configurable variables
├── db.bal                # Database access
└── kafka.bal             # Kafka producer / consumer logic
```

> The entry-point file is named after the service (`admin.bal`, `customer.bal`, `delivery.bal`, `notification.bal`, `order.bal`, `payment.bal`, `restaurant.bal`).

## Services

| Service      | Port | Folder                                   |
|--------------|------|------------------------------------------|
| Customer     | 9001 | `services/Customer/customer_service`     |
| Restaurant   | 9002 | `services/Restaurant/restaurant_service` |
| Order        | 9003 | `services/Order/order_service`           |
| Payment      | 9004 | `services/Payment/payment_service`       |
| Delivery     | 9005 | `services/Delivery/delivery_service`     |
| Notification | 9006 | `services/Notification/notification_service` |
| Admin        | 9007 | `services/Admin/admin_service`           |

## Kafka Topics

See [`kafka/topics.md`](kafka/topics.md) for the full table. Main topics: `orders.created`, `orders.status.changed`, `orders.cancelled`, `stock.reserved` / `stock.rejected`, `payments.completed`, `payments.failed`, `delivery.assigned`, `delivery.pickedup`, `delivery.completed`, plus `*.dlq` dead-letter topics.

## Running the Project

```bash
cp .env.example .env
docker compose up --build
```

## Status

Project scaffold is in place. Still to complete: service logic, `docker-compose.yml`, Dockerfiles, `kafka/create-topics.sh`, database init scripts, docs, and the UI.
