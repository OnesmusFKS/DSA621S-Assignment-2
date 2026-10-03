# Kafka topics (fill in)

| Topic | Producer | Consumer(s) | Partition key | Partitions |
|---|---|---|---|---|
| orders.created | order | restaurant, payment, notification, admin | orderId | |
| orders.status.changed | order | customer, delivery, notification, admin | orderId | |
| orders.cancelled | order | restaurant, payment, notification | orderId | |
| stock.reserved / stock.rejected | restaurant | order | orderId | |
| payments.completed | payment | order, notification, admin | orderId | |
| payments.failed | payment | order, notification | orderId | |
| delivery.assigned | delivery | order, notification | orderId | |
| delivery.pickedup | delivery | order, notification | orderId | |
| delivery.completed | delivery | order, notification, admin | orderId | |
| *.dlq | any | (manual review) | | |

TODO: create-topics script, retention, replication factor.
