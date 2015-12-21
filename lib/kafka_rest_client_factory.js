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
var KafkaRestClient = require('./kafka_rest_client');
var CBuffer = require('CBuffer');
var async = require('async');

var instance = null;

function KafkaRestClientFactory(options, callback) {
    var self = this;

    self.options = options;

    self.proxyHost = options.proxyHost;
    self.proxyPort = options.proxyPort;

    // map topic to address
    self.topicAddressMap = {};

    // map one KafkaRestClient per address
    self.addressClientMap = {};

    self.loadTopicMap(callback);

    if ('queueSizeBytes' in options.batchOptions) {
        self.queue = new BatchedMessagesQueue(options.batchOptions.queueSizeBytes);
    } else {
        self.queue = new BatchedMessagesQueue();
    }

    if ('flushCycleSecs' in options.batchOptions) {
        self.flushCycleSecs = options.batchOptions.flushCycleSecs;
    } else {
        self.flushCycleSecs = 1; // todo confirm
    }

    self.topicMapCycleSecs = 30 * 60;
    self.lastMapRefresh = new Date().getTime() - self.topicMapCycleSecs - 1;

    setInterval(function() { // eslint-disable-line
        self.flushMessagesAndRefreshTopicMap(callback);
    }, self.flushCycleSecs * 1000);
}

KafkaRestClientFactory.prototype.flushMessagesAndRefreshTopicMap = function flushMessagesAndRefreshTopicMap(callback) {
    var self = this;

    if ((new Date().getTime() - self.lastMapRefresh) > self.topicMapCycleSecs) {
        self.loadTopicMap(callback);
        self.lastMapRefresh = new Date().getTime();
    }

    var error = null;

    async.whilst(
        function checkQueue() {
            return !error && self.queue.hasNext();
        },
        function flushQueue() {
            // todo confirm
            var batchedMessage = self.queue.getBatchedMessage();
            var message = {
                topic: batchedMessage.topic,
                message: batchedMessage.getBatchedMessage,
                timeStamp: batchedMessage.timeStamp,
                type: 'batch'
            };

            self.getKafkaRestClient(batchedMessage.topic).produce(message, function cb(err) {
                error = err;
            });
        },
        function errors() {
            callback(error);
        }
    );

};

KafkaRestClientFactory.getKafkaRestClientFactory = function getKafkaRestClientFactory(options, callback) {
    if (!instance) {
        instance = new KafkaRestClientFactory(options, callback);
    }

    return instance;
};

KafkaRestClientFactory.prototype.formatTopicAddressMapJSON = function formatTopicAddressMapJSON(body, callback) {
    var self = this;

    var addresses = [];
    var topicAddressMap = self.topicAddressMap;

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

        addresses = urlArray;
        topicAddressMap = topicToUrlMapping;
    } catch(e) {
        callback(e);
    }

    return {
        addresses: addresses,
        topicAddressMap: topicAddressMap
    };
};

KafkaRestClientFactory.prototype.loadTopicMap = function loadTopicMap(callback) {
    var self = this;

    KafkaRestClient.getTopicRequestBody(self.proxyHost, self.proxyPort, function updateTopicAddressMap(err, res) {
        if (err) {
            callback(err);
        } else {
            var topicAddressMapAndAddresses = self.formatTopicAddressMapJSON(res, callback);

            if (Object.keys(topicAddressMapAndAddresses.topicAddressMap).length > 0) {
                self.topicAddressMap = topicAddressMapAndAddresses.topicAddressMap;

                for (var address in topicAddressMapAndAddresses.addresses) {
                    if (!(address in self.addressClientMap)) {
                        // todo RestClient options
                        self.addressClientMap[address] = new KafkaRestClient(self.options, callback);
                    }
                }
            }
        }
    });
};

KafkaRestClientFactory.prototype.getKafkaRestClient = function getKafkaRestClient(topic, callback) {
    var self = this;

    var address = null;
    if (topic in self.topicAddressMap) {
        address = self.topicAddressMap[address];
    }

    var client = null;
    if (address in self.addressClientMap) {
        client = self.addressClientMap[address];
    }

    return client;
};

function BatchedMessagesQueue(queueSizeBytes) {
    var self = this;
    var queueSize = queueSizeBytes || 10000000;
    self.queue = new CBuffer(queueSize / 100000);
}

BatchedMessagesQueue.prototype.addBatchedMessage = function addBatchedMessage(batchedMessage) {
    var self = this;
    self.queue.push(batchedMessage);
};

BatchedMessagesQueue.prototype.getBatchedMessage = function getBatchedMessage() {
    var self = this;
    return self.queue.shift();
};

BatchedMessagesQueue.prototype.hasNext = function hasNext() {
    var self = this;
    return self.queue.size > 0;
};

module.exports = KafkaRestClientFactory;

