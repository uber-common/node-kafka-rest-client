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

var instance = null;

function KafkaRestClientFactory(options, callback) {
    var self = this;

    self.options;

    self.proxyHost = options.proxyHost;
    self.proxyPort = options.proxyPort;

    # map topic to address
    self.topicAddressMap = {};

    # map one KafkaRestClient per address
    self.addressClientMap = {};

    self.loadTopicMap(callback);

    if ('queueSizeBytes' in options) {
        self.queue = new BatchedMessagesQueue(options.queueSizeBytes);
    } else {
        self.queue = new BatchedMessagesQueue();
    }

    if('flushCycleSecs' in options) {
        self.flushCycleSecs = options.flushCycleSecs;
    } else {
        self.flushCycleSecs = 1; //todo confirm
    }

    self.topicMapCycleSecs = 30 * 60;
    self.lastMapRefresh = new Date().getTime() - self.topicMapCycleSecs - 1;
};

KafkaRestClientFactory.prototype.flushMessagesAndRefreshTopicMap = function flushMessagesAndRefreshTopicMap() {
    var self = this;

    
};

KafkaRestClientFactory.prototype.getKafkaRestClientFactory = function getKafkaRestClientFactory(options) {
    if(!instance) {
        instance = new KafkaRestClientFactory(options);
    }

    return instance;
};

function formatTopicAddressMapJSON(body) {
    var self = this;

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
    } catch(e) {
        return self.topicAddressMap;
    }
};

KafkaRestClientFactory.prototype.loadTopicMap = function loadTopicMap(callback) {
    var self = this;

    KafkaRestClient.getAllTopicsRequest(self.proxyHost, self.proxyPort, function updateTopicAddressMap(err, res) {
        if (err) {
            callback(err);
        } else {
            var topicAddressMap = formatTopicAddressMapJSON(res);

            if(Object.keys(topicAddressMap).length > 0) {
                self.topicAddressMap = formatTopicAddressMapJSON(res);

                for (var address in new Set(self.topicAddressMap)) {
                    if(!(address in self.addressClientMap)) {
                        self.addressClientMap[address] = new KafkaRestClient(address, options, callback);
                    }
                }
            }
        }
    });
};

KafkaRestClientFactory.prototype.getKafkaRestClient(topic) = function getKafkaRestClient(topic, callback) {
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

function BatchedMessagesQueue(queueSizeBytes = 10000000) {
    var self = this;
    self.queue = new CBuffer(queue_size_in_bytes / 100000);
};

BatchedMessagesQueue.prototype.addBatchedMessage = function addBatchedMessage(batchedMessage, callback) {
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