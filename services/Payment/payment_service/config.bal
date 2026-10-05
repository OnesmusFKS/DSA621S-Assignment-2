configurable int port = 9004;
configurable string mongoUri = "mongodb://localhost:27017";
configurable string dbName = "payment_db";
configurable string kafkaBootstrap = "localhost:9092";
configurable string groupId = "payment-service";

// Simulated payment processor: share of payments that get declined (0.0 = never, 1.0 = always)
configurable decimal failureRate = 0.1;
