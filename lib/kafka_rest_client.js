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
/* global process */

var url = require('url');
var http = require('http');
var lbPool = require('lb_pool');
var os = require('os');
var utils = require('./utils');
/* eslint-disable no-unused-vars */
/* jshint -W098 */
var pkginfo = require('pkginfo')(module);
/* jshint +W089 */
/* eslint-enable no-unused-vars */

var supportContentType = {
    'binary': 'application/vnd.kafka.binary.v1',
    'json': 'application/vnd.kafka.json.v1+json',
    'batch': 'application/vnd.kafka.binary.batch.v1'
};

var emptyFunction = function EmptyFunction() {
};

var defaultAppNameEnvVar = 'UDEPLOY_SERVICE_NAME';
var defaultServiceNameHeader = 'kafka-rest-client-service-name';
var defaultVersionHeader = 'kafka-rest-client-version';
var defaultInstanceNameHeader = 'kafka-rest-client-instance-name';
var defaultInstanceNameEnvVar = 'UDEPLOY_DEPLOYMENT_NAME';

var defaultLocalAgentHost = 'localhost';
var defaultLocalAgentPort = 5390;
var defaultClusterUrl = 'LOGGING TOPICS';
var defaultDataClusterUrl = 'DATA TOPICS';
var hpTopicFormat = ['hp-', 'hp_', 'hp.'];
var totalWaitingTimeMs = 0;
// try connectWaitTimeMs.length times to connect to rest proxy
var connectWaitTimeMs = [0, 100, 3000, 10000, 30000, 60000, 300000, 600000];
var defaultReconnectRetryMs = 100;

String.isNullOrEmpty = function checkNullOrEmpty(value) {
    // based on https://codereview.stackexchange.com/questions/5572/string-isnullorempty-in-javascript
    return !value;
};
// Kafka 8 Rest Proxy Client.
// Initialization: takes host and port and send a GET call to get all the
// <topic, host:port> mapping.
// Produce: for sending request, forkafka client will send a POST call
function KafkaRestClient(options, callback) {
    var self = this;
    callback = callback || emptyFunction;
    self.clientType = options.clientType;
    self.enable = false;
    self.connecting = false;
    self.proxyHost = options.proxyHost;
    self.localAgentMode = options.localAgentMode;
    self.timeout = options.timeout || 1000;
    self.maxPending = options.maxPending || 500;
    self.urlToHttpClientMapping = {};
    self.cachedTopicToUrlMapping = {};
    self.proxyPort = options.proxyPort;
    self.initLineageOption(options);
    self.requestUrl = self.proxyHost + ':' + self.proxyPort;
    self.metadataWhitelist = null;
    if ('metadataWhitelist' in options) {
        self.metadataWhitelist = options.metadataWhitelist;
    }

    self.initHttpClient(options);
    self.topicDiscoveryTimes = 0;
    self.maxRetries = options.maxRetries || 3;

    self.discoverTopics(function discoverTopicsCallback(err, isDone) {
        if (err || !isDone) {
            self.enable = false;
            totalWaitingTimeMs = 0;
            self.scheduleConnect(1);
            // Will not return an error since there are retries to rest proxy
            return callback();
        }
        self.enable = true;
        return callback();
    });
    self.scheduleTopicMapRefresh();
    self.statsd = (options.statsd && typeof options.statsd.increment === 'function') ? options.statsd : null;
    self.metricsPrefix = 'kafka_rest_proxy.rest-client.' + os.hostname() + '.';
    self.localAgentHost = options.localAgentHost || defaultLocalAgentHost;
    self.localAgentPort = options.localAgentPort || defaultLocalAgentPort;
    self.produceInterval = options.produceInterval || -1;
}

KafkaRestClient.prototype.initHttpClient = function initHttpClient(options) {
    var self = this;
    if (self.clientType === 'AtLeastOnce') {
        self.mainHttpClient = new lbPool.Pool(http, [self.requestUrl], {
            /* eslint-disable camelcase */
            /* jshint camelcase: false */
            keep_alive: true,
            timeout: self.timeout,
            max_pending: self.maxPending
            /* jshint camelcase: true */
            /* eslint-enable camelcase */
        });
    } else {
        self.mainHttpClient = new lbPool.Pool(http, [self.requestUrl], {
            /* eslint-disable camelcase */
            /* jshint camelcase: false */
            keep_alive: true
            /* jshint camelcase: true */
            /* eslint-enable camelcase */
        });
    }
    self.proxyRefreshTime = options.refreshTime;

    if ('blacklistMigratorHttpClient' in options) {
        self.blacklistMigratorHttpClient = options.blacklistMigratorHttpClient;
    } else {
        self.blacklistMigratorHttpClient = null;
    }
};

KafkaRestClient.prototype.initLineageOption = function initLineageOption(options) {
    var self = this;
    if (options.serviceName) {
        // the application name for data lineage
        self.serviceName = options.serviceName;
    } else {
        if ('serviceNameEnv' in options) {
            self.serviceNameEnv = options.serviceNameEnv;
        } else {
            self.serviceNameEnv = defaultAppNameEnvVar;
        }
        /* eslint-disable no-process-env */
        self.serviceName = process.env[self.serviceNameEnv];
        /* eslint-enable no-process-env */

        if (String.isNullOrEmpty(self.serviceName)) {
            self.serviceName = utils.getServiceName();
        }
    }

    if ('serviceNameHeader' in options) {
        self.serviceNameHeader = options.serviceNameHeader;
    } else {
        self.serviceNameHeader = defaultServiceNameHeader;
    }

    if ('clientVersionHeader' in options) {
        self.clientVersionHeader = options.clientVersionHeader;
    } else {
        self.clientVersionHeader = defaultVersionHeader;
    }

    if ('instanceNameHeader' in options) {
        self.instanceNameHeader = options.instanceNameHeader;
    } else {
        self.instanceNameHeader = defaultInstanceNameHeader;
    }

    if ('instanceName' in options) {
        self.instanceName = options.instanceName;
    } else {
        if ('instanceNameEnv' in options) {
            self.instanceNameEnv = options.instanceNameEnv;
        } else {
            self.instanceNameEnv = defaultInstanceNameEnvVar;
        }
        /* eslint-disable no-process-env */
        self.instanceName = process.env[this.instanceNameEnv];
        if (String.isNullOrEmpty(self.instanceName)) {
            self.instanceName = utils.getLimitedLength(os.hostname());
        }
        /* eslint-enable no-process-env */
    }
    self.clientVersion = '[node,' + module.version + ']';

};
// produceInterval is for test purpose
// In production env, the interval is exponential back-off as 600 * Math.pow(5, retry + 1)
KafkaRestClient.prototype.getProduceInterval = function getProduceInterval(retry) {
    var self = this;
    if (self.produceInterval === -1) {
        if (self.clientType === 'AtLeastOnce') {
            return 1000;
        }
        return 600 * Math.pow(5, retry + 1);
    }
    return self.produceInterval;
};

KafkaRestClient.prototype.scheduleTopicMapRefresh = function scheduleTopicMapRefresh() {
    var self = this;
    if (self.proxyRefreshTime > 0) {
        /* eslint-disable no-undef,block-scoped-var */
        self.refreshIntervalId = setInterval(function refreshTopics() {
            totalWaitingTimeMs = 0;
            self.scheduleConnect(0);
        }, self.proxyRefreshTime);
        /* eslint-enable no-undef,block-scoped-var */
    }
};

// Reconnect to rest proxy if scheduleTopicMapRefresh fails
KafkaRestClient.prototype.scheduleConnect = function scheduleConnect(retry) {
    var self = this;
    if (retry < connectWaitTimeMs.length) {
        if (self.proxyRefreshTime > 0 && totalWaitingTimeMs + connectWaitTimeMs[retry] >= self.proxyRefreshTime) {
            return;
        }

        /* eslint-disable no-undef,block-scoped-var */
        self.reconnectId = setTimeout(function reconnect() {
            totalWaitingTimeMs += connectWaitTimeMs[retry];
            self.discoverTopics(function refreshTopicsCallback(err, isDone) {
                if (err === null && isDone) {
                    self.enable = true;
                } else if (retry < connectWaitTimeMs.length - 1) {
                    self.enable = false;
                    self.scheduleConnect(retry + 1);
                }
            });
        }, connectWaitTimeMs[retry]);
        /* eslint-enable no-undef,block-scoped-var */
    }
};

KafkaRestClient.prototype.discoverTopics = function discoverTopics(callback) {
    var self = this;
    self.connecting = true;
    this.getTopicRequestBody(self.proxyHost, self.proxyPort, function updateTopicToUrlMapping(err, res) {
        if (err) {
            self.connecting = false;
            return callback(err, false);
        }
        self.connecting = false;
        self.enable = true;
        self.cachedTopicToUrlMapping = self.getTopicToUrlMapping(res);
        self.topicDiscoveryTimes++;
        return callback(null, true);
    });
};

KafkaRestClient.prototype.getTopicRequestBody = function getTopicRequestBody(proxyHost, proxyPort, callback) {
    var self = this;
    var endpoint = '/topics';
    if (self.metadataWhitelist !== null) {
        endpoint += '?topics=' + self.metadataWhitelist.join();
    }
    self.mainHttpClient.get(endpoint, function handleGetCall(err, res, body) {
        if (err) {
            return callback(err, null);
        }
        callback(null, body);
    });
};

KafkaRestClient.prototype.getTopicToUrlMapping = function getTopicToUrlMapping(body) {
    var self = this;
    /* eslint-disable no-try-catch */
    try {
        var urlToTopicsJson = JSON.parse(body);
        var topicToUrlMapping = {};
        var urlArray = Object.keys(urlToTopicsJson);
        for (var i = 0; i < urlArray.length; i++) {
            var reqUrl = urlArray[i];
            var topicsArray = urlToTopicsJson[reqUrl];
            for (var j = 0; j < topicsArray.length; j++) {
                topicToUrlMapping[topicsArray[j]] = reqUrl;
            }
        }
        return topicToUrlMapping;
    } catch (e) {
        return self.cachedTopicToUrlMapping;
    }
    /* eslint-enable no-try-catch */
};

KafkaRestClient.prototype.isHeatPipeTopic = function isHeatPipeTopic(topic) {
    return hpTopicFormat.indexOf(topic.substring(0, 3)) > -1;
};

KafkaRestClient.prototype.goToLocalAgent = function goToLocalAgent(topic) {
    var self = this;
    return (self.clientType !== 'AtLeastOnce' && self.isHeatPipeTopic(topic)) ||
        (self.clientType === 'AtLeastOnce' && self.localAgentMode.toUpperCase() === 'ALWAYS');
};

// ProduceMessage {
// string topic,
// binary message,
// string type,
// long timeStamp
// }
KafkaRestClient.prototype.produce = function produce(produceMessage, callback) {
    var self = this;
    self.produceWithRetry(produceMessage, 0, callback);
    if (self.statsd) {
        self.statsd.increment(self.metricsPrefix + produceMessage.topic + '.produced');
    }
};

KafkaRestClient.prototype.produceWithRetry = function produceWithRetry(produceMessage, retry, callback) {
    var self = this;

    if (self.enable) {
        // push topic to whitelist if needed
        if (self.metadataWhitelist !== null && self.metadataWhitelist.indexOf(produceMessage.topic) < 0) {
            self.metadataWhitelist.push(produceMessage.topic);
        }
        if (produceMessage.topic in self.cachedTopicToUrlMapping) {
            self.tryMakeRequest(produceMessage, retry, callback);
        } else if (defaultDataClusterUrl in self.cachedTopicToUrlMapping && self.clientType !== 'AtLeastOnce') {
            self.cachedTopicToUrlMapping[produceMessage.topic] = self.cachedTopicToUrlMapping[defaultDataClusterUrl];
            self.produceWithRetry(produceMessage, 0, callback);
        } else if (self.goToLocalAgent(produceMessage.topic)) {
            self.tryMakeRequest(produceMessage, self.maxRetries - 1, callback);
        } else if (defaultClusterUrl in self.cachedTopicToUrlMapping && self.clientType !== 'AtLeastOnce') {
            self.cachedTopicToUrlMapping[produceMessage.topic] = self.cachedTopicToUrlMapping[defaultClusterUrl];
            self.produceWithRetry(produceMessage, 0, callback);
        } else if (self.clientType !== 'AtLeastOnce') {
            return callback(new Error('No default url for logging topics - ' + produceMessage.topic));
        } else {
            return callback(new Error('No url for topics - ' + produceMessage.topic));
        }
    } else if (self.connecting) {
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function waitForInitDone() {
            self.produceWithRetry(produceMessage, retry, callback);
        }, defaultReconnectRetryMs);
        /* eslint-enable no-undef,block-scoped-var */
    } else if (self.goToLocalAgent(produceMessage.topic)) {
        self.tryMakeRequest(produceMessage, self.maxRetries - 1, callback);
    } else {
        return callback(new Error('Kafka Rest Client is not enabled yet.'));
    }
};

KafkaRestClient.prototype.tryMakeRequest = function tryMakeRequest(produceMessage, retry, callback) {
    var self = this;

    var pathUrl = self.cachedTopicToUrlMapping[produceMessage.topic];
    if (retry >= self.maxRetries - 1 && self.maxRetries > 1 && self.goToLocalAgent(produceMessage.topic)) {
        pathUrl = self.localAgentHost + ':' + self.localAgentPort;
    }
    if (pathUrl === undefined) {
        pathUrl = self.localAgentHost + ':' + self.localAgentPort;
    }

    var httpClient;
    if (self.urlToHttpClientMapping[pathUrl] === undefined) {
        if (self.clientType === 'AtLeastOnce') {
            self.urlToHttpClientMapping[pathUrl] = new lbPool.Pool(http, [pathUrl], {
                /* eslint-disable camelcase */
                /* jshint camelcase: false */
                keep_alive: false,
                timeout: self.timeout,
                max_pending: self.maxPending
                /* jshint camelcase: true */
                /* eslint-enable camelcase */
            });
        } else {
            self.urlToHttpClientMapping[pathUrl] = new lbPool.Pool(http, [pathUrl], {
                /* eslint-disable camelcase */
                /* jshint camelcase: false */
                keep_alive: false
                /* jshint camelcase: true */
                /* eslint-enable camelcase */
            });
        }
    }
    httpClient = self.urlToHttpClientMapping[pathUrl];

    var reqOpts = url.parse('http://' + pathUrl);
    reqOpts.method = 'POST';
    reqOpts.path = '/topics/' + produceMessage.topic;
    reqOpts.headers = {
        'Content-Type': supportContentType[produceMessage.type],
        'TimeStamp': produceMessage.timeStamp.toString()
    };
    if (produceMessage.key !== undefined && produceMessage.key !== '') {
        reqOpts.headers['Record-Key'] = produceMessage.key;
    }
    if (self.clientType === 'AtLeastOnce') {
        reqOpts.headers['Request-Type'] = 'sync';
    }
    reqOpts.headers[self.serviceNameHeader] = self.serviceName;
    reqOpts.headers[self.clientVersionHeader] = self.clientVersion;
    reqOpts.headers[self.instanceNameHeader] = self.instanceName;

    var time = new Date();
    httpClient.post(reqOpts, produceMessage.message, function handlePostCall(err, res, body) {
        var metricPrefix = self.metricsPrefix + produceMessage.topic;
        if (self.statsd && typeof self.statsd.timing === 'function') {
            self.statsd.timing(metricPrefix + '.latency', time);
        }

        if (err || (res && res.statusCode >= 500 && res.statusCode < 600)) {
            return self.handleError5xx(produceMessage, retry, metricPrefix, err, callback);
        }
        if (res.statusCode >= 400 && res.statusCode < 500) {
            return self.handleError4xx(produceMessage, res, callback);
        }
        if (self.clientType !== 'AtLeastOnce' ||
            (self.clientType === 'AtLeastOnce' && pathUrl !== self.localAgentHost + ':' + self.localAgentPort)) {
            return callback(null, body);
        }
        callback(new Error('Send to local agent for topic - ' + produceMessage.topic));
    });
};

/* jshint maxparams: 5 */
KafkaRestClient.prototype.handleError5xx = function handleError1(produceMessage, retry, metricPrefix, err, callback) {
    var self = this;

    if (retry < self.maxRetries - 1) {
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function delayedProduce() {
            self.produceWithRetry(produceMessage, retry + 1, callback);
        }, self.getProduceInterval(retry));
        /* eslint-enable no-undef,block-scoped-var */

        if (self.statsd) {
            self.statsd.increment(metricPrefix + '.retry');
        }
    } else if (self.goToLocalAgent(produceMessage.topic) &&
        retry === self.maxRetries - 1 && self.maxRetries > 1) {
        /* eslint-disable no-undef,block-scoped-var */
        setTimeout(function delayedProduce() {
            self.tryMakeRequest(produceMessage, self.maxRetries, callback);
        }, 100);
        /* eslint-enable no-undef,block-scoped-var */
    } else {
        if (err !== null && typeof err === 'object') {
            // attempt field in err return by lb_pool lib has circular structure
            // will cause problem when we try to JSON.stringify it
            err.attempt = null;
        }
        if (self.statsd) {
            self.statsd.increment(metricPrefix + '.error');
        }
        return void callback(err);
    }
};

KafkaRestClient.prototype.handleError4xx = function handleError2(produceMessage, res, callback) {
    var self = this;

    if (self.goToLocalAgent(produceMessage.topic) && self.maxRetries > 1) {
        self.tryMakeRequest(produceMessage, self.maxRetries - 1, callback);
    } else {
        return void callback(new Error('Got statusCode ' + res.statusCode));
    }
};

KafkaRestClient.prototype.close = function close() {
    // Trying to close kafka rest client, need to cancel timer scheduler.
    var self = this;
    self.enable = false;
    self.connecting = false;
    if (self.proxyRefreshTime > 0) {
        // Trying to cancel TimeInterval
        /* eslint-disable no-undef,block-scoped-var */
        clearInterval(self.refreshIntervalId);
        /* eslint-enable no-undef,block-scoped-var */
        self.proxyRefreshTime = 0;
    }
    if (self.reconnectId) {
        /* eslint-disable no-undef,block-scoped-var */
        clearTimeout(self.reconnectId);
        /* eslint-enable no-undef,block-scoped-var */
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
