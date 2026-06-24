## secure_transaction_pipeline 

Every time you tap your card or hit "pay" online, a chain of backend systems evaluates that transaction in milliseconds. These systems communicate by sending events to each other, scoring each payment for fraud before it clears.

In this project, we will build a secure transaction pipeline with Go and Apache Kafka. Two microservices communicate through events to ingest, score, and store payment transactions with fraud detection.

