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

var hostName = require('os').hostname();
var KafkaRestClient = require('./kafka_rest_client');
var MigratorBlacklistClient = require('./migrator_blacklist_client');
var Buffer = require('buffer').Buffer;

function KafkaProducer(options, callback) { // eslint-disable-line
    // Trying to init KafkaProducer
    var self = this;
    self.proxyHost = options.proxyHost || 'localhost';
    self.maxRetries = options.maxRetries || 1;
    // proxyPort is must have, otherwise KafkaProducer is disabled.
    if ('proxyPort' in options) {
        self.proxyPort = options.proxyPort;
        if ('proxyRefreshTime' in options) {
            self.proxyRefreshTime = options.proxyRefreshTime;
        } else {
            // default refresh time is 30 min.
            self.proxyRefreshTime = 1000 * 60 * 30;
        }
        self.blacklistMigratorHttpClient = null;
        if ('blacklistMigrator' in options && 'blacklistMigratorUrl' in options) {
            if (options.blacklistMigrator) {
                self.blacklistMigratorHttpClient = new
                    MigratorBlacklistClient(options.blacklistMigratorUrl);
            }
        }
        if ('shouldAddTopicToMessage' in options) {
            self.shouldAddTopicToMessage = options.shouldAddTopicToMessage;
        } else {
            self.shouldAddTopicToMessage = false;
        }
        if ('statsd' in options) {
            self.statsd = options.statsd;
        } else {
            self.statsd = false;
        }
        if ('batching' in options) {
            self.batching = options.batching;
        } else {
            self.batching = false;
        }

        if (self.batching) {
            self.maxMessageSizeBytes = 1000100; // 1mb per a message
            self.maxBatchSizeBytes = options.maxBatchSizeBytes || 26214400; // default 25mb per a topic
            self.topicToBatchQueue = {}; // map of topic name to BatchMessage
            self.flushCycleSecs = 1; // flush a topic's batch message every second
        }

        self.init = true;
    } else {
        self.init = false;
        self.enable = false;
    }
}

KafkaProducer.prototype.connect = function connect(onConnect) {
    var self = this;
    if (self.init) {
        var restClientOptions = {
            proxyHost: self.proxyHost,
            proxyPort: self.proxyPort,
            refreshTime: self.proxyRefreshTime,
            blacklistMigratorHttpClient: self.blacklistMigratorHttpClient,
            maxRetries: self.maxRetries,
            statsd: self.statsd
        };
        self.restClient = new KafkaRestClient(restClientOptions,
            function onConnectRestClient(err) {
                if (!err) {
                    self.enable = true;
                    onConnect(err);
                } else {
                    self.enable = false;
                    onConnect(err);
                }
            }
        );
    } else {
        onConnect(new Error('Kafka producer is not initialized.'));
    }
};

KafkaProducer.prototype.produce = function produce(topic, message, timeStamp, callback) {
    var self = this;

    if (self.restClient) {
        if (self.batching) {
            self.batch(topic, message, timeStamp, callback);
        } else {
            var produceMessage = self.getProduceMessage(topic, message, timeStamp, 'binary');
            self.restClient.produce(produceMessage, function handleResponse(err, res) {
                if (callback) {
                    callback(err, res);
                }
            });
        }
    } else if (callback) {
        callback(new Error('Kafka Rest Client is not initialized!'));
    }
};

// Add message to topic's BatchMessage in cache or add new topic to cache
// Flush topic's BatchMessage on flushCycleSecs interval or if greater than maxBatchSizeBytes
KafkaProducer.prototype.batch = function batch(topic, message, timeStamp, callback) {
    var self = this;
    var currTime = new Date().getTime();
    if (topic in self.topicToBatchQueue) {
        var queue = self.topicToBatchQueue[topic];
        if ((queue.messageBatch.sizeBytes + message.length) > self.maxBatchSizeBytes ||
                (currTime - queue.lastFlush) > self.flushCycleSecs * 1000) {
            var produceMessage = self.getProduceMessage(topic, queue.messageBatch.getBatchedMessage(), timeStamp, 'batch');
            self.restClient.produce(produceMessage, function handleResponse(err, res) {
                if (callback) {
                    callback(err, res);
                }
            });

            queue.messageBatch = new MessageBatch(topic);
            queue.lastFlush = new Date().getTime();
        }

        queue.messageBatch.addMessage(message);
    } else {
        var messageBatch = new MessageBatch(topic);
        messageBatch.addMessage(message);

        var batchQueue = {
            messageBatch: messageBatch,
            lastFlush: currTime
        };

        self.topicToBatchQueue[topic] = batchQueue;
    }
    callback();
};

KafkaProducer.prototype.flushEntireCache = function flushEntireCache(callback) {
    var self = this;

    var handleResponse = function handleResponse(err, res) {
        if (callback) {
            callback(err, res);
        }
    };

    for (var topic in self.topicToBatchQueue) {
        if (self.topicToBatchQueue.hasOwnProperty[topic]) {
            var queue = self.topicToBatchQueue[topic];
            var timeStamp = new Date().getTime();
            var produceMessage = self.getProduceMessage(topic, queue.messageBatch.getBatchedMessage(), timeStamp, 'batch');
            self.restClient.produce(produceMessage, handleResponse);

            queue.messageBatch = new MessageBatch(topic);
            queue.lastFlush = new Date().getTime();
        }
    }
};

KafkaProducer.prototype.getProduceMessage = function getProduceMessage(topic, message, timeStamp, type) {
    var produceMessage = {};
    produceMessage.topic = topic;
    produceMessage.message = message;
    produceMessage.timeStamp = timeStamp;
    produceMessage.type = type;
    return produceMessage;
};

KafkaProducer.prototype.logLine = function logLine(topic, message, callback) {
    var self = this;
    var timeStamp = Date.now() / 1000.0;
    self.logLineWithTimeStamp(topic, message, timeStamp, callback);
};

KafkaProducer.prototype.logLineWithTimeStamp = function logLine(topic, message, timeStamp, callback) {
    var self = this;
    var wholeMessage = JSON.stringify(self.getWholeMsg(topic, message, timeStamp));
    self.produce(topic, wholeMessage, timeStamp, function handleResponse(err, res) {
        if (callback) {
            callback(err, res);
        }
    });
};

function ProducedMessage(msg, ts, host) {
    var message = {
        ts: ts,
        host: host,
        msg: msg
    };
    return message;
}

function ProducedTopicMessage(msg, ts, host, topic) {
    var message = {
        ts: ts,
        host: host,
        msg: msg,
        topic: topic
    };
    return message;
}

KafkaProducer.prototype.getWholeMsg = function getWholeMsg(topic, message, timeStamp) {
    var self = this;
    if (self.shouldAddTopicToMessage) {
        return new ProducedTopicMessage(message, timeStamp, hostName, topic);
    }
    return new ProducedMessage(message, timeStamp, hostName);
};

KafkaProducer.prototype.close = function close(callback) {
    var self = this;
    if (self.batching) {
        self.flushEntireCache(callback);
    }
    self.enable = false;
    if (self.restClient) {
        self.restClient.close();
    }
};

// The batch message payload should follow:
// 4 BE bytes number of messages + 4 BE bytes size of message + actual message
function MessageBatch(topic) {
    var self = this;

    self.payload = null;
    self.timestamp = new Date().getTime();
    self.numMessages = 0;
    self.sizeBytes = 0;
    self.topic = topic;
}

MessageBatch.prototype.addMessage = function addMessage(message) {
    var self = this;

    var messageBinary = new Buffer(message);
    var sizeBuf = new Buffer(4);
    sizeBuf.writeInt32BE(messageBinary.length, 0);

    if (self.payload) {
        self.payload = Buffer.concat([self.payload, sizeBuf, messageBinary]);
    } else {
        self.payload = Buffer.concat([sizeBuf, messageBinary]);
    }

    self.numMessages += 1;
    self.sizeBytes += messageBinary.length;
};

MessageBatch.prototype.getBatchedMessage = function getBatchedMessage() {
    var self = this;

    var sizeBuf = new Buffer(4);
    sizeBuf.writeInt32BE(self.numMessages, 0);

    var message = Buffer.concat([sizeBuf, self.payload]);

    return message;
};

module.exports = KafkaProducer;
