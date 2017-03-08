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
var KafkaDataProducer = require('../lib/kafka_data_producer');
var ProducerRecord = require('../lib/producer_record');

var KafkaRestProxyServer = require('./lib/test_kafka_rest_proxy');

test('Kafka data producer with correct configs.', function testKafkaDataProducer(assert) {
    var PORT = 4444;
    var server = new KafkaRestProxyServer(PORT);
    server.start();

    var configs1 = {
        proxyHost: 'localhost',
        proxyPort: PORT
    };
    var producer1 = new KafkaDataProducer(configs1);
    producer1.connect(onConnect1);

    function onConnect1() {
        assert.equal(producer1.producer.init, false);
    }

    var configs2 = {
        clientType: 'AtLeastOnce',
        proxyHost: 'localhost',
        proxyPort: PORT
    };
    var producer2 = new KafkaDataProducer(configs2);
    producer2.connect(onConnect2);

    function onConnect2() {
        assert.equal(producer2.producer.restClient.enable, true);
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function stopTest() {
            assert.equal(producer2.producer.restClient.enable, true);
            producer2.close();
            server.stop();
            assert.end();
        }, 600);
        /* eslint-enable no-undef,block-scoped-var */
    }
});

test('Kafka producer could write with produce.', function testKafkaProducer(assert) {
    var PORT = 4444;
    var server = new KafkaRestProxyServer(PORT);
    server.start();

    var configs = {
        clientType: 'AtLeastOnce',
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0
    };
    var producer = new KafkaDataProducer(configs);
    producer.connect(onConnect);

    function onConnect() {
        assert.equal(producer.producer.restClient.enable, true);
        async.parallel([
            function testWithValue(next) {
                producer.produceSync('testTopic0', new ProducerRecord({value: 'value'}), generateSuccessCheck(next));
            },
            function testWithKeyAndValue(next) {
                producer.produceSync('testTopic1', new ProducerRecord({key: 'test_key', value: 'value'}), generateSuccessCheckWithKey(next));
            },
            function testWithoutValue(next) {
                producer.produceSync('testTopic10', new ProducerRecord({key: 'test_key'}), generateErrorCheck(next));
            }
        ], function end() {
            producer.close();
            server.stop();
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

    function generateSuccessCheckWithKey(next) {
        return function onSuccessResponse(err, res) {
            assert.equal(producer.producer.restClient.enable, true);
            assert.equal(err, null);
            assert.equal(res, '{ version : 1, Status : SENT, message : {}, key : test_key}');
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
