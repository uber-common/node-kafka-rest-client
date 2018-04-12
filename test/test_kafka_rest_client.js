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

var rewire = require('rewire');
var test = require('tape');
var async = require('async');
var KafkaRestProxyServer = require('./lib/test_kafka_rest_proxy');
var KafkaRestClient = rewire('../lib/kafka_rest_client');
var os = require('os');

KafkaRestClient.__set__({
    'KafkaRestClient.prototype.getTopicRequestBody': function getTopicRequestBodyMock(proxyHost, proxyPort, callback) {
        var messages = {
            'localhost:1111': ['testTopic0', 'testTopic1', 'testTopic2', 'testTopic3', 'hp.testTopic1'],
            'localhost:2222': ['testTopic4', 'testTopic5', 'testTopic6', 'testTopic7'],
            'localhost:15380': ['LOGGING TOPICS']
        };
        callback(null, JSON.stringify(messages));
    }
});

function getProduceMessage(topic, message, ts, type) {
    var produceMessage = {};
    produceMessage.topic = topic;
    produceMessage.message = message;
    produceMessage.timeStamp = ts;
    produceMessage.type = type;
    return produceMessage;
}

test('KafkaRestClient can discover topics', function testKafkaRestClientTopicDiscovery(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 4444,
        proxyRefreshTime: 0
    };
    var restClient = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3
    });
    assert.equal(Object.keys(restClient.cachedTopicToUrlMapping).length, 10);
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic0, 'localhost:1111');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic1, 'localhost:1111');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic2, 'localhost:1111');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic3, 'localhost:1111');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic4, 'localhost:2222');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic5, 'localhost:2222');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic6, 'localhost:2222');
    assert.equal(restClient.cachedTopicToUrlMapping.testTopic7, 'localhost:2222');
    restClient.close();
    assert.end();
});

test('KafkaRestClient handle failed post with retries', function testKafkaRestClientHanldeFailedPostCall(assert) {
    var server = new KafkaRestProxyServer(5555);

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 1111,
        proxyRefreshTime: 0
    };
    var timeStamp = Date.now() / 1000.0;
    var restClient = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        localAgentPort: 5555,
        produceInterval: 100,
        refreshTime: configs.proxyRefreshTime
    });

    async.parallel([
        function test1(next) {
            restClient.produce(getProduceMessage('testTopic0', 'msg0', timeStamp, 'binary'),
                function assertHttpErrorReason(err) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    next();
                });
        },
        function test2(next) {
            restClient.produce(getProduceMessage('hp.testTopic1', 'msg1', timeStamp, 'binary'),
                function assertErrorThrows(err) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    next();
                });
        }
    ], function secondPart() {
        server.start();
        async.parallel([
            function test3(next) {
                restClient.produce(getProduceMessage('testTopic0', 'msg1', timeStamp, 'binary'),
                    function assertErrorThrows(err) {
                        assert.equal(err.reason, 'connect ECONNREFUSED');
                        next();
                    });
            },
            function test4(next) {
                restClient.produce(getProduceMessage('hp.testTopic1', 'msg1', timeStamp, 'binary'),
                    function assertErrorThrows(err) {
                        assert.equal(err, null);
                        next();
                    });
            }
        ], function end() {
            restClient.close();
            server.stop();
            assert.end();
        });
    });
});

test('KafkaRestClient handle not cached topics', function testKafkaRestClientHanldeNotCachedTopics(assert) {
    var server = new KafkaRestProxyServer(15380);

    var configs = {
        proxyHost: 'localhost',
        proxyPort: 1111,
        proxyRefreshTime: 0
    };
    var timeStamp = Date.now() / 1000.0;
    var restClient = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        produceInterval: 100
    });

    async.parallel([
        function test1(next) {
            restClient.produce(getProduceMessage('hp-testTopic-not-in-map', 'msg0', timeStamp, 'binary'),
                function assertErrorThrows(err) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    next();
                });
        },
        function test2(next) {
            restClient.produce(getProduceMessage('testTopic-not-in-map', 'msg0', timeStamp, 'binary'),
                function assertHttpErrorReason(err) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    server.start();
                    restClient.produce(getProduceMessage('testTopic-not-in-map', 'msg0', timeStamp, 'binary'),
                        function assertHttpErrorReason2(err2) {
                            assert.equal(err2, null);
                            next();
                        });
                });
        }
    ], function end() {
        restClient.close();
        server.stop();
        assert.end();
    });
});

test('KafkaRestClient handle post with blacklist client', function testKafkaRestClientHanldeFailedPostCall(assert) {
    var configs = {
        proxyHost: 'localhost',
        proxyPort: 1111,
        proxyRefreshTime: 0
    };
    var timeStamp = Date.now() / 1000.0;
    var restClient = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        produceInterval: 100
    });

    async.parallel([
        function test1(next) {
            restClient.produce(getProduceMessage('testTopic0', 'msg0', timeStamp, 'binary'),
                function assertHttpErrorReason(err) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    next();
                });
        },
        function test2(next) {
            restClient.produce(getProduceMessage('testTopic1', 'msg0', timeStamp, 'binary'),
                function assertErrorThrows(err, resp) {
                    assert.equal(err.reason, 'connect ECONNREFUSED');
                    assert.equal(resp, undefined);
                    next();
                });
        }
    ], function end() {
        restClient.close();
        assert.end();
    });
});

function verifyHeader(assert, PORT, restClient) {
    var topicName = 'LOGGING TOPICS';
    var timeStamp = Date.now() / 1000.0;

    /* jshint maxparams: 5 */
    function OverriddenHttpClient(expectedServiceNameHeader, expectedServiceName, expectedClientVersionHeader,
                                  expectedInstanceNameHeader, expectedInstanceName) {
        this.expectedServiceNameHeader = expectedServiceNameHeader;
        this.expectedServiceName = expectedServiceName;
        this.postMethodCalled = false;
        this.expectedClientVersionHeader = expectedClientVersionHeader;
        this.expectedInstanceNameHeader = expectedInstanceNameHeader;
        this.expectedInstanceName = expectedInstanceName;
    }

    OverriddenHttpClient.prototype.post = function post(reqOpts, msg, cb) {
        assert.true(this.expectedServiceNameHeader in reqOpts.headers);
        assert.true(this.expectedInstanceNameHeader in reqOpts.headers);
        assert.equal(reqOpts.headers[this.expectedServiceNameHeader], this.expectedServiceName);
        assert.equal(reqOpts.headers[this.expectedInstanceNameHeader], this.expectedInstanceName);
        assert.true(reqOpts.headers[this.expectedClientVersionHeader].indexOf('node') >= 0);
        this.postMethodCalled = true;
    };

    var mockedHttpClient = new OverriddenHttpClient(restClient.serviceNameHeader,
        restClient.serviceName, restClient.clientVersionHeader, restClient.instanceNameHeader,
        restClient.instanceName);
    var urlPath = 'localhost:' + PORT.toString();
    restClient.urlToHttpClientMapping = {};
    restClient.urlToHttpClientMapping[urlPath] = mockedHttpClient;
    restClient.produce(getProduceMessage(topicName, 'bla', timeStamp, 'binary'),
        function assertErrorThrows(err) {
            assert.true(err !== null);
        });
    assert.true(mockedHttpClient.postMethodCalled);
}

/* eslint-disable max-statements */
test('KafkaRestClient can apply lineage header config correctly',
    function testKafkaRestClientLineageHeaderConfig(assert) {
        // Lets define a port we want to listen to
        var PORT = 15380;

        var configs = {
            proxyHost: 'localhost',
            proxyPort: PORT,
            proxyRefreshTime: 0
        };

        /* global process */
        /* eslint-disable no-process-env */

        // case 1: pass in serviceName directly. use it
        process.env.UDEPLOY_APP_ID = 'Pickle!';
        process.env.UDEPLOY_DEPLOYMENT_NAME = 'Beth';
        var restClient = new KafkaRestClient({
            proxyHost: configs.proxyHost,
            proxyPort: configs.proxyPort,
            refreshTime: configs.proxyRefreshTime,
            maxRetries: 3,
            serviceName: 'Rick and Morty',
            instanceName: 'production #4'
        });
        assert.equal(restClient.serviceNameHeader, 'kafka-rest-client-service-name');
        assert.equal(restClient.serviceName, 'Rick and Morty');
        assert.equal(restClient.instanceName, 'production #4');
        assert.true(restClient.clientVersion.indexOf('node') > -1);
        verifyHeader(assert, PORT, restClient);

        // case 2: pass in nothing, and no environment var,
        //  result: generate default with client version, service name, and instance name
        process.env.UDEPLOY_APP_ID = '';
        process.env.UDEPLOY_DEPLOYMENT_NAME = '';
        var restClient2 = new KafkaRestClient({
            proxyHost: configs.proxyHost,
            proxyPort: configs.proxyPort,
            refreshTime: configs.proxyRefreshTime,
            maxRetries: 3
        });
        assert.equal(restClient2.serviceNameHeader, 'kafka-rest-client-service-name');
        assert.equal(restClient2.serviceNameEnv, 'UDEPLOY_APP_ID');
        assert.assert(restClient2.serviceName.indexOf('node-kafka-rest-client') > -1);
        assert.assert(restClient2.instanceName.indexOf(os.hostname()) > -1);
        assert.true(restClient2.clientVersion.indexOf('node') > -1);
        verifyHeader(assert, PORT, restClient2);

        // case 3: pass in nothing, but environment var has value,
        //  result: generate name by environment variable value
        process.env.UDEPLOY_APP_ID = 'Pickle!';
        process.env.UDEPLOY_DEPLOYMENT_NAME = 'Planet-Express';
        var restClient3 = new KafkaRestClient({
            proxyHost: configs.proxyHost,
            proxyPort: configs.proxyPort,
            refreshTime: configs.proxyRefreshTime,
            maxRetries: 3
        });
        assert.equal(restClient3.serviceNameHeader, 'kafka-rest-client-service-name');
        assert.equal(restClient3.serviceNameEnv, 'UDEPLOY_APP_ID');
        assert.equal(restClient3.instanceNameEnv, 'UDEPLOY_DEPLOYMENT_NAME');
        assert.equal(restClient3.serviceName, 'Pickle!');
        assert.assert(restClient3.instanceName, 'Planet-Express');
        assert.true(restClient3.clientVersion.indexOf('node') > -1);

        verifyHeader(assert, PORT, restClient3);

        // case 4: pass in customized environment variable name, and customize header name
        //  result: generate name by customized environment variable value with new header
        process.env.UDEPLOY_APP_ID = 'Pickle!';
        process.env.RICK_APP_ID = 'Meeseek';
        process.env.UDEPLOY_DEPLOYMENT_NAME = 'Planet-Express';
        process.env.MORTY_INSTANCE_ID = 'Citadel';
        var restClient4 = new KafkaRestClient({
            proxyHost: configs.proxyHost,
            proxyPort: configs.proxyPort,
            refreshTime: configs.proxyRefreshTime,
            maxRetries: 3,
            serviceNameEnv: 'RICK_APP_ID',
            serviceNameHeader: 'lineage-source',
            instanceNameEnv: 'MORTY_INSTANCE_ID',
            instanceNameHeader: 'ricklantis-mixup'
        });
        assert.equal(restClient4.serviceNameHeader, 'lineage-source');
        assert.equal(restClient4.serviceNameEnv, 'RICK_APP_ID');
        assert.equal(restClient4.serviceName, 'Meeseek');
        assert.equal(restClient4.instanceNameEnv, 'MORTY_INSTANCE_ID');
        assert.equal(restClient4.instanceNameHeader, 'ricklantis-mixup');
        assert.equal(restClient4.instanceName, 'Citadel');
        assert.true(restClient4.clientVersion.indexOf('node') > -1);

        verifyHeader(assert, PORT, restClient4);

        // case 5: pass in everything in config
        //  result: the instance name and service name provided is used
        process.env.UDEPLOY_APP_ID = 'Pickle!';
        process.env.RICK_APP_ID = 'Meeseek';
        process.env.UDEPLOY_DEPLOYMENT_NAME = 'Planet-Express';
        process.env.MORTY_INSTANCE_ID = 'Citadel';
        var restClient5 = new KafkaRestClient({
            proxyHost: configs.proxyHost,
            proxyPort: configs.proxyPort,
            refreshTime: configs.proxyRefreshTime,
            maxRetries: 3,
            serviceName: 'Meeseek',
            serviceNameEnv: 'RICK_APP_ID',
            serviceNameHeader: 'lineage-source',
            instanceName: 'production #1',
            instanceNameHeader: 'ricklantis-mixup',
            clientVersionHeader: 'x-client-id'
        });
        assert.equal(restClient5.serviceNameHeader, 'lineage-source');
        assert.equal(restClient5.serviceName, 'Meeseek');
        assert.equal(restClient5.instanceNameHeader, 'ricklantis-mixup');
        assert.equal(restClient5.instanceName, 'production #1');
        assert.equal(restClient5.clientVersionHeader, 'x-client-id');
        assert.true(restClient5.clientVersion.indexOf('node') > -1);

        verifyHeader(assert, PORT, restClient5);
        assert.end();

    });
/* eslint-enable max-statements */
