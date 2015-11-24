# node-kafka-rest-client

A rest client for producing JSON and binary messages to kafka.

e

```js
var KafkaProducer = require("./lib/kafka_producer");
var configs = {
    proxyHost: 'localhost',
    proxyPort: 18083,
    proxyRefreshTime: 0
};

var kafkaProducer = new KafkaProducer(configs);
kafkaProducer.connect(onConnect);

function onConnect(err) {
    /* eslint-disable no-undef,no-console,block-scoped-var */
    if (!err) {
        console.log('KafkaRestClient connected to kafka');
    } else {
        console.log('KafkaRestClient could not connect to kafka');
    }
    /* eslint-enable no-undef,no-console,block-scoped-var */
}

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
