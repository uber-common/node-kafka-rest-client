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

    function getProduceMessage(topic, message, ts, type) {
        var produceMessage = {};
        produceMessage.topic = topic;
        produceMessage.message = message;
        produceMessage.timeStamp = ts;
        produceMessage.type = type;
        return produceMessage;
    }

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
    var topicName = "LOGGING TOPICS";
    var timeStamp = Date.now() / 1000.0;

    function OverriddenHttpClient(expectedHeader, expectedAppName, expectedClientVersionHeader){
        this.expectedHeader = expectedHeader;
        this.expectedAppName = expectedAppName;
        this.postMethodCalled = false;
        this.expectedClientVersionHeader = expectedClientVersionHeader;
    }

    OverriddenHttpClient.prototype.post = function post(reqOpts, msg, cb){
        assert.true(this.expectedHeader in reqOpts.headers);
        assert.equal(reqOpts.headers[this.expectedHeader], this.expectedAppName);
        assert.true(reqOpts.headers[this.expectedClientVersionHeader].indexOf('node') >= 0);
        this.postMethodCalled = true;
        //cb();
    };

    var mockedHttpClient = new OverriddenHttpClient(restClient.lineageHeader,
        restClient.applicationName, restClient.clientVersionHeader);
    var urlPath = "localhost:" + PORT.toString();
    restClient.urlToHttpClientMapping = {};
    restClient.urlToHttpClientMapping[urlPath] = mockedHttpClient;
    restClient.produce(getProduceMessage(topicName, 'bla', timeStamp, 'binary'),
        function assertErrorThrows(err) {
            console.log(err.reason);
        });
    assert.true(mockedHttpClient.postMethodCalled);
}
test('KafkaRestClient can apply lineage header config correctly', function testKafkaRestClientLineageHeaderConfig(assert) {
    //Lets define a port we want to listen to
    var PORT=15380;

    var configs = {
        proxyHost: 'localhost',
        proxyPort: PORT,
        proxyRefreshTime: 0
    };

    /* global process */
    /* eslint-disable no-process-env */

    // case 1: pass in applicationName directly. use it
    process.env.UDEPLOY_APP_ID = 'Pickle!';
    var restClient = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3,
        applicationName: 'Rick and Morty'
    });
    assert.equal(restClient.lineageHeader, 'x-uber-source');
    assert.equal(restClient.applicationName, 'Rick and Morty');
    verifyHeader(assert, PORT, restClient);

    // case 2: pass in nothing, and no environment var,
    //  result: generate default with client version and hostname
    process.env.UDEPLOY_APP_ID = '';
    var restClient2 = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3
    });
    assert.equal(restClient2.lineageHeader, 'x-uber-source');
    assert.equal(restClient2.applicationNameEnv, 'UDEPLOY_APP_ID');
    assert.assert(restClient2.applicationName.indexOf('node-kafka-rest-client') > -1);
    assert.assert(restClient2.applicationName.indexOf(os.hostname()) > -1);
    verifyHeader(assert, PORT, restClient2);

    // case 3: pass in nothing, but environment var has value,
    //  result: generate name by environment variable value
    process.env.UDEPLOY_APP_ID = 'Pickle!';
    var restClient3 = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3
    });
    assert.equal(restClient3.lineageHeader, 'x-uber-source');
    assert.equal(restClient3.applicationNameEnv, 'UDEPLOY_APP_ID');
    assert.equal(restClient3.applicationName, 'Pickle!');
    verifyHeader(assert, PORT, restClient3);

    // case 4: pass in customized environment variable name, and customize header name
    //  result: generate name by customized environment variable value with new header
    process.env.UDEPLOY_APP_ID = 'Pickle!';
    process.env.RICK_APP_ID = 'Meeseek';
    var restClient4 = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3,
        applicationNameEnv: 'RICK_APP_ID',
        lineageHeader: 'lineage-source'
    });
    assert.equal(restClient4.lineageHeader, 'lineage-source');
    assert.equal(restClient4.applicationNameEnv, 'RICK_APP_ID');
    assert.equal(restClient4.applicationName, 'Meeseek');

    verifyHeader(assert, PORT, restClient4);

    // case 5: pass in customized environment variable name, and customize header name
    //  result: generate name by customized environment variable value with new header
    process.env.UDEPLOY_APP_ID = 'Pickle!';
    process.env.RICK_APP_ID = 'Meeseek';
    var restClient5 = new KafkaRestClient({
        proxyHost: configs.proxyHost,
        proxyPort: configs.proxyPort,
        refreshTime: configs.proxyRefreshTime,
        maxRetries: 3,
        applicationNameEnv: 'RICK_APP_ID',
        lineageHeader: 'lineage-source',
        clientVersionHeader: 'x-client-id'
    });
    assert.equal(restClient5.lineageHeader, 'lineage-source');
    assert.equal(restClient5.applicationNameEnv, 'RICK_APP_ID');
    assert.equal(restClient5.applicationName, 'Meeseek');
    assert.equal(restClient5.clientVersionHeader, 'x-client-id');

    verifyHeader(assert, PORT, restClient5);
    assert.end();


});
