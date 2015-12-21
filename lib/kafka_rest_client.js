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

var url = require('url');
var http = require('http');
var lbPool = require('lb_pool');
var os = require('os');

var supportContentType = {
    'binary': 'application/vnd.kafka.binary.v1',
    'json': 'application/vnd.kafka.json.v1+json'
};

var emptyFunction = function EmptyFunction() {
};

/*
  Kafka 8 Rest Proxy Client.
  Initialization: takes host and port and send a GET call to get all the
  <topic, host:port> mapping.
  Produce: for sending request, forkafka client will send a POST call
*/
function KafkaRestClient(options, callback) {
    var self = this;
    callback = callback || emptyFunction;
    self.enable = false;
    self.connecting = false;
    self.proxyHost = options.proxyHost;
    self.urlToHttpClientMapping = {};
    self.cachedTopicToUrlMapping = {};
    self.proxyPort = options.proxyPort;
    self.requestUrl = self.proxyHost + ':' + self.proxyPort;
    self.mainHttpClient = new lbPool.Pool(http, [self.requestUrl], {
        keep_alive: true // eslint-disable-line
    });
    self.proxyRefreshTime = options.refreshTime;
    if ('blacklistMigratorHttpClient' in options) {
        self.blacklistMigratorHttpClient = options.blacklistMigratorHttpClient;
    } else {
        self.blacklistMigratorHttpClient = null;
    }
    self.topicDiscoveryTimes = 0;
    self.maxRetries = options.maxRetries;

    self.discoverTopics(function discoverTopicsCallback(err, isDone) {
        if (err || !isDone) {
            self.enable = false;
            callback(new Error('Failed to discover topics mapping'));
        } else {
            self.enable = true;
            callback();
        }
    });
    self.scheduleTopicMapRefresh();

    self.statsd = (options.statsd && typeof options.statsd.increment === 'function') ? options.statsd : null;
    self.metricsPrefix = 'kafka_rest_proxy.rest-client.' + os.hostname() + '.';

    self.maxMessageSizeBytes = 1000100;
    self.maxBatchSizeBytes = options.maxBatchSizeBytes || 1000100;
    self.flushTimeSec = 1;

    self.enableBatch = options.enableBatch || true;
    self.messageQueues = {};
}

KafkaRestClient.prototype.scheduleTopicMapRefresh = function scheduleTopicMapRefresh() {
    var self = this;
    if (self.proxyRefreshTime > 0) {
        self.refreshIntervalId = setInterval(function refreshTopics() { // eslint-disable-line
            self.discoverTopics(function refreshTopicsCallback(err, isDone) {
                if (err === null && isDone) {
                    self.enable = true;
                }
            });
        }, self.proxyRefreshTime);
    }
};

KafkaRestClient.prototype.discoverTopics = function discoverTopics(callback) {
    var self = this;
    self.connecting = true;
    this.getTopicRequestBody(self.proxyHost, self.proxyPort, function updateTopicToUrlMapping(err, res) {
        if (err) {
            self.connecting = false;
            callback(err, false);
        } else {
            self.connecting = false;
            self.enable = true;
            self.cachedTopicToUrlMapping = self.getTopicToUrlMapping(res);
            self.topicDiscoveryTimes++;
            callback(err, true);
        }
    });
};

KafkaRestClient.prototype.getTopicRequestBody = function getTopicRequestBody(proxyHost, proxyPort, callback) {
    var self = this;
    self.mainHttpClient.get('/topics', function handleGetCall(err, res, body) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, body);
        }
    });
};

/*
 ProduceMessage {
   string topic,
   binary message,
   string type,
   long timeStamp
 }
*/
KafkaRestClient.prototype.produce = function produce(produceMessage, callback) {
    var self = this;
    if (self.blacklistMigratorHttpClient) {
        self.blacklistMigratorHttpClient.blacklistTopic(produceMessage.topic,
            function blacklistTopicCallback(isBlacklisted) {
            if (isBlacklisted) {
                self.produceWithRetry(produceMessage, 0, callback);
            } else {
                callback(null, 'Topic is not blacklisted, not produce data to kafka rest proxy.');
            }
        });
    } else {
        self.produceWithRetry(produceMessage, 0, callback);
        if (self.statsd) {
            self.statsd.increment(self.metricsPrefix + produceMessage.topic + '.produced');
        }
    }
};

KafkaRestClient.prototype.produceWithRetry = function produceWithRetry(produceMessage, retry, callback) {
    var self = this;

    if (self.enable) {
        if (produceMessage.topic in self.cachedTopicToUrlMapping) {
            if (self.enableBatch) {
                self.enqueueMessage(produceMessage.topic, produceMessage.message);
            } else {
                self.postPayload(produceMessage.topic, produceMessage.message);
            }
        } else {
            return callback(new Error('Topics Not Found.'));
        }
    } else if (self.connecting) {
        setTimeout(function waitForInitDone() { // eslint-disable-line
            self.produceWithRetry(produceMessage, retry, callback);
        }, 100);
    } else {
        callback(new Error('Kafka Rest Client is not enabled yet.'));
    }

};

KafkaRestClient.prototype.postPayload = function postPayload(topic, payload) {
    var self = this;

    var pathUrl = self.cachedTopicToUrlMapping[produceMessage.topic];
    var httpClient;
    if (self.urlToHttpClientMapping[pathUrl] === undefined) {
        self.urlToHttpClientMapping[pathUrl] = new lbPool.Pool(http, [pathUrl], {
            keep_alive: true // eslint-disable-line
        });
    }
    httpClient = self.urlToHttpClientMapping[pathUrl];

    var reqOpts = url.parse('http://' + pathUrl);
    reqOpts.method = 'POST';
    reqOpts.path = '/topics/' + produceMessage.topic;
    reqOpts.headers = {
        'Content-Type': supportContentType[produceMessage.type],
        'TimeStamp': produceMessage.timeStamp.toString()
    };
    var time = new Date();
    httpClient.post(reqOpts, payload, function handlePostCall(err, res, body) {
        var metricPrefix = self.metricsPrefix + produceMessage.topic;

        if (self.statsd && typeof self.statsd.timing === 'function') {
            self.statsd.timing(metricPrefix + '.latency', time);
        }

        if (err) {
            // Only retry with exception: Connection refused
            if (err.reason === 'connect ECONNREFUSED' && retry < self.maxRetries) {
                // 3sec, 15sec, 75sec retry
                setTimeout(function delayedProduce() { // eslint-disable-line
                    self.produceWithRetry(produceMessage, retry + 1, callback);
                }, 600 * Math.pow(5, retry + 1));

                if (self.statsd) {
                    self.statsd.increment(metricPrefix + '.retry');
                }
            } else {
                callback(err);

                if (self.statsd) {
                    self.statsd.increment(metricPrefix + '.error');
                }
            }
        } else {
            callback(null, body);
        }
    });
};

KafkaRestClient.prototype.close = function close() {
    // Trying to close kafka rest client, need to cancel timer scheduler.
    var self = this;
    self.enable = false;
    self.connecting = false;
    if (self.proxyRefreshTime > 0) {
        // Trying to cancel TimeInterval
        clearInterval(self.refreshIntervalId); // eslint-disable-line
        self.proxyFreshTime = 0;
    }
    self.mainHttpClient.close();
    if (self.blacklistMigratorHttpClient) {
        self.blacklistMigratorHttpClient.close();
    }
    Object.keys(self.urlToHttpClientMapping).forEach(function closeHttpClient(reqUrl) {
        self.urlToHttpClientMapping[reqUrl].close();
    });
};

module.exports = KafkaRestClient;
