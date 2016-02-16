# node-kafka-rest-client

A rest client for producing JSON and binary messages to kafka.

## Usage

```js
var KafkaRestClient = require('kafka-rest-client');
var configs = {
    proxyHost: 'localhost',
    proxyPort: 18084
};

var kafkaRestClient = new KafkaRestClient(kafkaRestClientOptions, callback);
kafkaRestClient.connect(callback);

kafkaRestClient.produce(topicName, 'Example Kafka Message', callback);

```

### Options

KafkaProducer constructor accepts these options:
  - `proxyHost` - Rest proxy hostname to produce kafka messages (default: `localhost`)
  - `proxyPort` - Rest proxy port to produce kafka messages (required)

##Install

    npm install kafka-rest-client

## Running tests

Tests are run using `npm`:

    npm run test
    
## Running lint

Tests are run using `npm`:

    npm run lint

## MIT Licenced
