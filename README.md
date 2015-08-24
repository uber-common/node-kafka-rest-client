# node-kafka-rest-client

A rest client for producing messages to kafka.

This library only producing json and binary messages to kafka. If you want to consume
from kafka please use https://github.com/uber/nodesol or https://github.com/uber/kafka-node

## Usage

```js
var KafkaProducer = require("./lib/kafka_producer");
var configs = {
    proxyHost: 'localhost',
    proxyPort: 18083,
    proxyRefreshTime: 0
};

var kafkaProducer = new KafkaProducer(configs);

function msgCallback(err, res) {
    if (err) {
        console.log(err);
    } else {
        console.log(res);
    }
}

// Timestamped log entry with host
kafkaProducer.logLine('test', 'Important message', msgCallback);
//Just dump log, no callback.
kafkaProducer.logLine('test', 'Important message #1');
// Just sends the raw message directly to kafka
kafkaProducer.produce('test', 'Important message #2', msgCallback);

```

### Options

KafkaProducer constructor accepts these options:
  - `proxyHost` - Rest proxy hostname to produce kafka messages (default: `localhost`)
  - `proxyPort` - Rest proxy port to produce kafka messages (required)
  - `proxyRefreshTime` - time in ms to refresh topic to request url mapping (default: 30min)
## Running tests

Tests are run using `npm`:

    npm run test
    
## Running lint

Tests are run using `npm`:

    npm run lint

## MIT Licenced
