// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

'use strict';

var test = require('tape');
var async = require('async');
var KafkaProducer = require('../lib/kafka_producer');
var KafkaVersion = require('../package.json').version;

var KafkaRestProxyServer = require('./lib/test_kafka_rest_proxy');

test('Kafka producer could write with produce.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function test1(next) {
                producer.produce('testTopic0', 'Important message', generateSuccessCheck(next));
            },
            function test2(next) {
                /* eslint-disable camelcase */
                /* jshint camelcase: false */
                producer.log_line('testTopic1', 'Important message', generateSuccessCheck(next));
                /* eslint-enable camelcase */
                /* jshint camelcase: true */
            },
            function test3(next) {
                producer.logLine('testTopic10', 'Important message', generateErrorCheck(next));
            }
        ], function end() {
            server.stop();
            producer.close();
            assert.end();
        });
    }

    function generateSuccessCheck(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
            next();
        };
    }

    function generateErrorCheck(next) {
        return function onTopicNotFoundError(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Topics Not Found.');
                }
            }, Error);
            assert.equal(res, undefined);
            next();
        };
    }
});

test('Kafka producer could write with batched produce.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0,
        batching: true
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function test1(next) {
                producer.produce('testTopic0', 'Important message', generateSuccessCheck(next));
            },
            function test2(next) {
                producer.logLine('testTopic1', 'Important message', generateSuccessCheck(next));
            },
            function test3(next) {
                producer.logLine('testTopic10', 'Important message', generateErrorCheck(next));
            }
        ], function end() {
            server.stop();
            producer.close();
            assert.end();
        });
    }

    function generateSuccessCheck(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
            next();
        };
    }

    function generateErrorCheck(next) {
        return function onTopicNotFoundError(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Topics Not Found.');
                }
            }, Error);
            assert.equal(res, undefined);
            next();
        };
    }
});

test('Kafka producer properly handles large messages.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0,
        batching: true
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    // Specifically test to make sure we batch messages at the 100k
    // limit correctly
    var almostTooLargeMessage = new Array(100000 - 7).join('a');
    var tooLargeMessage = almostTooLargeMessage + 'b';
    var topic = 'testTopic0';

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);

        // This message is too large, it shouldn't end up in the batch
        producer.produce(topic, tooLargeMessage, 0);
        assert.equal(producer.producer.topicToBatchQueue[topic].numMessages, 0);

        // This message is exactly at the max size, it should end up in the batch
        producer.produce(topic, almostTooLargeMessage, 0);
        assert.equal(producer.producer.topicToBatchQueue[topic].numMessages, 1);
        assert.equal(producer.producer.topicToBatchQueue[topic].sizeBytes, almostTooLargeMessage.length + 8);

        // This message is too large, it shouldn't end up in the
        // batch, and shouldn't trigger the batch to flush.
        producer.produce(topic, tooLargeMessage, 0);
        assert.equal(producer.producer.topicToBatchQueue[topic].numMessages, 1);

        server.stop();
        producer.close();
        assert.end();
    }
});

test('Kafka producer could write with blacklisted batched produce.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0,
        batching: true,
        batchingBlacklist: [
            'testTopic1'
        ]
    };
    var producer = new KafkaProducer(configs);
    // Override the batch function to always return an error, `testTopic1` should not hit this,
    // but `testTopic0` should
    producer.producer.batch = function batchOverride(topic, message, timestamp, callback) {
        callback('This errors');
    };
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function test1(next) {
                producer.produce('testTopic0', 'Important message', generateErrorCheck(next));
            },
            function test2(next) {
                producer.logLine('testTopic1', 'Important message', generateSuccessCheck(next));
            }
        ], function end() {
            server.stop();
            producer.close();
            assert.end();
        });
    }

    function generateSuccessCheck(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
            next();
        };
    }

    function generateErrorCheck(next) {
        return function onTopicNotFoundError(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Topics Not Found.');
                }
            }, Error);
            assert.equal(res, undefined);
            next();
        };
    }
});

test('Kafka producer could write with whitelisted batched produce.', function testKafkaProducer(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0,
        batching: true,
        batchingWhitelist: [
            'testTopic0'
        ]
    };
    var producer = new KafkaProducer(configs);
    // Override the batch function to always return an error, `testTopic1` should not hit this,
    // but `testTopic0` should
    producer.producer.batch = function batchOverride(topic, message, timestamp, callback) {
        callback('This errors');
    };
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function test1(next) {
                producer.produce('testTopic0', 'Important message', generateErrorCheck(next));
            },
            function test2(next) {
                producer.logLine('testTopic1', 'Important message', generateSuccessCheck(next));
            }
        ], function end() {
            server.stop();
            producer.close();
            assert.end();
        });
    }

    function generateSuccessCheck(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
            next();
        };
    }

    function generateErrorCheck(next) {
        return function onTopicNotFoundError(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Topics Not Found.');
                }
            }, Error);
            assert.equal(res, undefined);
            next();
        };
    }
});

test('Kafka producer could write with produce and blacklist.', function testKafkaProducer(assert) {
    var restServer = new KafkaRestProxyServer(4444);
    restServer.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function test1(next) {
                producer.produce('testTopic0', 'Important message', generateSuccessCheck(next));
            },
            function test2(next) {
                producer.logLine('testTopic10', 'Important message', generateErrorCheck(next));
            }
        ], function end() {
            restServer.stop();
            producer.close();
            assert.end();
        });
    }

    function generateSuccessCheck(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}}');
            next();
        };
    }

    function generateErrorCheck(next) {
        return function onTopicNotFoundError(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Topics Not Found.');
                }
            }, Error);
            assert.equal(res, undefined);
            next();
        };
    }
});

test('Kafka producer handle unavailable proxy.', function testKafkaProducerHandleUnavailableProxy(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 5555
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, false);
        function onClientNotEnableError(err, res) {
            assert.throws(function throwError() {
                if (err) {
                    throw new Error('Kafka Rest Client is not enabled yet.');
                }
            }, Error);
            assert.equal(res, undefined);
        }

        producer.logLine('avro650', 'Important message', onClientNotEnableError);
        producer.close();
        assert.end();
    }
});

test('Kafka producer refresh.', function testKafkaProducerTopicRefresh(assert) {
    var server2 = new KafkaRestProxyServer(6666);
    server2.start();

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 6666,
        proxyRefreshTime: 1000
    };

    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    assert.equal(producer.producer.restClient.topicDiscoveryTimes, 0);
    function onConnect() {
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function wait1() {
            assert.equal(producer.producer.restClient.topicDiscoveryTimes, 1);
        }, 500);
        setTimeout(function wait2() {
            assert.equal(producer.producer.restClient.topicDiscoveryTimes, 2);
        }, 1500);
        setTimeout(function wait3() {
            assert.equal(producer.producer.restClient.topicDiscoveryTimes, 3);
        }, 2500);
        setTimeout(function stopTest2() {
            producer.close();
            server2.stop();
            assert.end();
        }, 3000);
        /* eslint-enable no-undef,block-scoped-var */
    }
});

test('Test get whole msg', function testKafkaProducerGetWholeMsgFunction(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 8888,
        shouldAddTopicToMessage: true
    };
    var testTimeStamp = Date.now() / 1000.0;
    var hostName = require('os').hostname();
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    function onConnect() {
        var testTopic = 'testTopic0';
        var testMsg = 'testMsg0';
        var wholeMsg = producer.producer.getWholeMsg(testTopic, testMsg, testTimeStamp);

        assert.equal(wholeMsg.host, hostName);
        assert.equal(wholeMsg.msg, testMsg);
        assert.equal(wholeMsg.topic, testTopic);
        assert.equal(wholeMsg.ts, testTimeStamp);
        assert.end();
        producer.close();
    }
});

test('Test generate audit msg', function testKafkaProducerGenerateAuditMsg(assert) {
    var server = new KafkaRestProxyServer(4444);
    server.start();

    var PORT = 4444;
    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        enableAudit: true,
        auditTimeBucketIntervalInSec: 1
    };
    var producer = new KafkaProducer(configs);
    producer.connect(onConnect);
    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        for (var i = 0; i < 5120000; i++) {
            producer.produce('testTopic0', 'Important message', Date.now() / 1000.0);
        }
        producer.produce('testTopic1', 'Important message', Date.now() / 1000.0);
        producer.produce('testTopic1', 'Important message', Date.now() / 1000.0);
        producer.logLine('testTopic2', 'Important message');
        producer.logLine('testTopic2', 'Important message');
        /* eslint-disable camelcase */
        /* jshint camelcase: false */
        producer.log_line('testTopic2', 'Important message');

        // C3 and C2 use the same functions to generate audit msgs
        var auditMsgs = producer.producer._generateAuditMsgs(producer.auditTier, producer.auditDatacenter,
            producer.producer.topicToMsgcntMaps);
        var cntTestTopic0 = 0;
        var cntTestTopic1 = 0;
        var cntTestTopic2 = 0;
        for (i = 0; i < auditMsgs.length; i++) {
            var auditMsg = auditMsgs[i];
            var json = JSON.parse(auditMsg);
            if (json.topic_count.testTopic0) {
                cntTestTopic0 += json.topic_count.testTopic0;
            }
            if (json.topic_count.testTopic1) {
                cntTestTopic1 += json.topic_count.testTopic1;
            }
            if (json.topic_count.testTopic2) {
                cntTestTopic2 += json.topic_count.testTopic2;
            }
            assert.equal(json.version, KafkaVersion);
        }

        assert.equal(cntTestTopic0, 5120000);
        assert.equal(cntTestTopic1, 2);
        assert.equal(cntTestTopic2, 3);
        assert.end();
        /* jshint camelcase: true */
        /* eslint-enable camelcase */

        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function stopTest1() {
            server.stop();
            producer.close();
        }, 2000);
        /* eslint-enable no-undef,block-scoped-var */
    }
});

test('Test calc timeBeginInSec', function testKafkaProducerCalcTimeBeginInSec(assert) {
    var timeBeginInSec = Math.floor((Date.now() / 1000) / 600) * 600;
    assert.equal(timeBeginInSec % 600, 0);
    assert.end();
});

test('kafkaProducer handle failed rest proxy connection', function testKafkaProducerHanldeFailedRPConnection(assert) {
    var server = new KafkaRestProxyServer(8082);

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 8082
    };

    var kafkaProducer = new KafkaProducer(configs);
    kafkaProducer.connect(function assertErrorThrows() {
        assert.equal(kafkaProducer.producer.restClient.enable, false);
        server.start();
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function stopTest1() {
            assert.equal(kafkaProducer.producer.restClient.enable, true);
            kafkaProducer.close();
            server.stop();
            assert.end();
        }, 600);
        /* eslint-enable no-undef,block-scoped-var */
    });
});

test('kafkaProducer handle no meta data situation', function testKafkaProducerHanldeNoMetaData(assert) {
    var server = new KafkaRestProxyServer(5390);

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 1111
    };
    var kafkaProducer = new KafkaProducer(configs);
    kafkaProducer.connect(function assertErrorThrows() {
        assert.equal(kafkaProducer.producer.restClient.enable, false);
        async.parallel([
            function test1(next) {
                kafkaProducer.produce('testTopic0', 'Important message', Date.now() / 1000.0,
                    function assertErrorThrows1(err) {
                        assert.equal(err.message, 'Kafka Rest Client is not enabled yet.');
                        next();
                    });
            },
            function test2(next) {
                kafkaProducer.produce('hp_testTopic0', 'Important message', Date.now() / 1000.0,
                    function assertErrorThrows2(err) {
                        assert.equal(err.reason, 'connect ECONNREFUSED');
                        next();
                    });
            }
        ], function end() {
            server.start();
            kafkaProducer.produce('testTopic0', 'Important message', Date.now() / 1000.0,
                function assertErrorThrows3(err) {
                    assert.equal(err.message, 'Kafka Rest Client is not enabled yet.');
                    kafkaProducer.produce('hp_testTopic0', 'Important message', Date.now() / 1000.0,
                        function assertNoErrorThrows(err2) {
                            assert.equal(err2, null);
                            kafkaProducer.close();
                            server.stop();
                            assert.end();
                        });
                });
        });
    });
});
