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
var MessageBatch = require('./message_batch');
var clearInterval = require('timers').clearInterval;
var setInterval = require('timers').setInterval;

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
            // default 100kb buffer cache per a topic
            self.maxBatchSizeBytes = options.maxBatchSizeBytes || 100000;
            self.topicToBatchQueue = {}; // map of topic name to MessageBatch

            self.flushCycleSecs = options.flushCycleSecs || 1; // flush a topic's batch message every second
            var flushCache = function flushCache() { // eslint-disable-line
                self.flushEntireCache();
            };
            self.flushInterval = setInterval(flushCache, self.flushCycleSecs * 1000); // eslint-disable-line
        }

        self.init = true;
    } else {
        self.init = false;
        self.enable = false;
    }

    self.pendingWrites = 0;
    self.closing = false;
    self.closeCallback = null;
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

KafkaProducer.prototype.produce = function produce(topic, message, timeStamp, callback, skipBatching) {
    var self = this;

    var produceMessage = null;

    if (self.closing) {
        if (callback) {
            callback(new Error('cannot produce() to closed KafkaRestProducer'));
        } // TODO: else { self.logger.error() }
        return;
    }

    if (self.restClient) {
        if (skipBatching) {
            produceMessage = self.getProduceMessage(topic, message, timeStamp, 'binary');
            self._produce(produceMessage, callback);
        } else if (self.batching) {
            self.batch(topic, message, timeStamp, callback);
        } else {
            produceMessage = self.getProduceMessage(topic, message, timeStamp, 'binary');
            self._produce(produceMessage, callback);
        }
    } else if (callback) {
        callback(new Error('Kafka Rest Client is not initialized!'));
    }
};

KafkaProducer.prototype._produce = function _produce(msg, callback) {
    var self = this;

    self.pendingWrites++;
    self.restClient.produce(msg, onResponse);

    function onResponse(err, result) {
        if (callback) {
            callback(err, result);
        }

        self.pendingWrites--;
        if (self.closing) {
            self._tryClose();
        }
    }
};

// Add message to topic's BatchMessage in cache or add new topic to cache
// Flush topic's BatchMessage on flushCycleSecs interval or if greater than maxBatchSizeBytes
KafkaProducer.prototype.batch = function batch(topic, message, timeStamp, callback) {
    var self = this;

    if (self.closing) {
        if (callback) {
            callback(new Error('cannot batch() to closed KafkaRestProducer'));
        } // TODO: else { self.logger.error() }
        return;
    }

    var messageBatch;
    var messageLength = message.length;
    var produceMessage = function produceMessage(msg) {
        return self.getProduceMessage(topic, msg, timeStamp, 'batch');
    };

    if (!self.topicToBatchQueue[topic]) {
        messageBatch = new MessageBatch(self.maxBatchSizeBytes);
        self.topicToBatchQueue[topic] = messageBatch;
    } else {
        messageBatch = self.topicToBatchQueue[topic];
    }

    if ((messageBatch.sizeBytes + messageLength + 4) > self.maxBatchSizeBytes) {
        self._produceBatch(messageBatch, topic, timeStamp);
    }

    // Do not add to buffer if message is larger than max buffer size, produce directly
    if (messageLength > self.maxBatchSizeBytes) {
        self._produce(produceMessage(message), callback);
    } else {
        self.topicToBatchQueue[topic].addMessage(message, callback);
    }
};

KafkaProducer.prototype._produceBatch =
function _produceBatch(messageBatch, topic, timeStamp, callback) {
    var self = this;

    var msg = messageBatch.getBatchedMessage();
    messageBatch.resetBatchedMessage();
    var pmsg = self.getProduceMessage(topic, msg, timeStamp, 'batch');

    self._produce(pmsg, onProduced);

    function onProduced(err, res) {
        for (var i = 0; i < messageBatch.pendingCallbacks.length; i++) {
            messageBatch.pendingCallbacks[i](err, res);
        }

        if (callback) {
            callback(err, res);
        }
    }
};

KafkaProducer.prototype.flushEntireCache = function flushEntireCache(callback) {
    var self = this;

    var pending = 0;
    var keys = Object.keys(self.topicToBatchQueue);
    for (var i = 0; i < keys.length; i++) {
        var topic = keys[i];
        var messageBatch = self.topicToBatchQueue[topic];

        if (messageBatch.numMessages > 0) {
            var timeStamp = Date.now();
            pending++;

            self._produceBatch(messageBatch, topic, timeStamp, onProduced);
        }
    }

    if (pending === 0 && callback) {
        callback(null);
    }

    function onProduced(err) {
        if (err && pending !== 0 && callback) {
            pending = 0;
            return callback(err);
        }

        if (--pending === 0 && callback) {
            callback(null);
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

    self.enable = false;
    self.closing = true;
    self.closeCallback = callback;

    if (self.flushInterval) {
        clearInterval(self.flushInterval);
    }

    if (self.batching) {
        self.flushEntireCache();
    }

    self._tryClose();
};

KafkaProducer.prototype._tryClose = function _tryClose() {
    var self = this;

    if (self.pendingWrites !== 0) {
        return;
    }

    if (self.restClient) {
        self.restClient.close();
    }

    if (self.closeCallback) {
        self.closeCallback();
    }
};

module.exports = KafkaProducer;

