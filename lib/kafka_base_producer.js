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

var Buffer = require('buffer').Buffer;
var hostName = require('os').hostname();
var KafkaVersion = require('../package.json').version;
var KafkaRestClient = require('./kafka_rest_client');
var MessageBatch = require('./message_batch');
var clearInterval = require('timers').clearInterval;
var setInterval = require('timers').setInterval;
var defaultLocalAgentPort = 5390;

function KafkaBaseProducer(options, producerType) { // eslint-disable-line
    // Trying to init KafkaBaseProducer
    var self = this;

    if (producerType === 'KafkaDataProducer' && options.clientType !== 'AtLeastOnce' ||
        producerType === 'KafkaProducer' && options.clientType === 'AtLeastOnce') {
        self.init = false;
        self.enable = false;
        return;
    }

    // proxyPort is must have, otherwise KafkaProducer is disabled.
    if ('proxyPort' in options) {
        self.producerType = producerType;
        if (producerType === 'KafkaDataProducer') {
            self.clientType = 'AtLeastOnce';
            self.localAgentMode = options.localAgentMode || 'Never';
        } else {
            self.clientType = 'Regular';
            self.localAgentMode = 'Always';
        }

        self.proxyHost = options.proxyHost || 'localhost';
        self.proxyPort = options.proxyPort;

        self.maxRetries = options.maxRetries || 3;
        self.produceInterval = options.produceInterval || -1;
        self.localAgentPort = options.localAgentPort || defaultLocalAgentPort;
        self.timeout = options.timeout || 1000;
        self.maxPending = options.maxPending || 300;
        if ('proxyRefreshTime' in options) {
            self.proxyRefreshTime = options.proxyRefreshTime;
        } else {
            // default refresh time is 30 min.
            self.proxyRefreshTime = 1000 * 60 * 30;
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
            self.batching = true;
        }
        if ('batchingWhitelist' in options) {
            self.batchingWhitelist = options.batchingWhitelist || [];
        } else {
            self.batchingBlacklist = options.batchingBlacklist || [];
        }

        self.enableAudit = options.enableAudit || true;
        self.auditTopicName = options.auditTopicName || 'chaperone2-audit-rest-proxy-client';
        self.auditTier = options.auditTier || 'rest-proxy-client-nodejs';
        self.auditDatacenter = options.auditDatacenter || 'unknown';
        self.auditTimeBucketIntervalInSec = options.auditTimeBucketIntervalInSec || 600;

        self.enableAuditC3 = options.enableAuditC3 || true;
        self.auditTopicNameC3 = options.auditTopicNameC3 || 'chaperone3-audit-rest-proxy-client';
        self.hpMsgTsOffset = 8;
        self.hpMsgMinLength = self.hpMsgTsOffset + 8;
        self.auditTierAtProduce = 'produce-nodejs';
        self.auditTierAtBatch = 'batch-nodejs';
        self.timestampSkewLimitInSec = options.timestampSkewLimitInSec || 345600;

        self.topicLevelOpts = options.topicLevelOpts;

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

        if (self.enableAudit) {
            // map of topic name to current msg count, as: {ts1:{tpc1:cnt1, tpc2:cnt2}, ts2:{tpc1:cnt1, tpc2:cnt2}}
            self.topicToMsgcntMaps = {};

            var produceAuditMsg = function doProduceAuditMsg() {
                var auditMsgs = self._generateAuditMsgs(self.auditTier, self.auditDatacenter, self.topicToMsgcntMaps);
                for (var i = 0; i < auditMsgs.length; i++) {
                    var auditMsg = auditMsgs[i];
                    self.produce(self.auditTopicName, auditMsg, (Date.now() / 1000));
                }
                self.topicToMsgcntMaps = {}; // reset the msg count map for next round of auditing
            };

            self.auditInterval = setInterval(produceAuditMsg, self.auditTimeBucketIntervalInSec * 1000);
        }

        if (self.enableAuditC3) {
            // map of topic name to current msg count, as: {ts1:{tpc1:cnt1, tpc2:cnt2}, ts2:{tpc1:cnt1, tpc2:cnt2}}
            self.topicToMsgcntMapsAtProduce = {};
            self.topicToMsgcntMapsAtBatch = {};

            var produceC3AuditMsg = function doProduceAuditMsg() {
                var auditMsgsAtProduce = self._generateAuditMsgs(self.auditTierAtProduce, self.auditDatacenter,
                    self.topicToMsgcntMapsAtProduce);
                for (var i = 0; i < auditMsgsAtProduce.length; i++) {
                    var auditMsgAtProduce = auditMsgsAtProduce[i];
                    self.produce(self.auditTopicNameC3, auditMsgAtProduce, (Date.now() / 1000));
                }
                self.topicToMsgcntMapsAtProduce = {}; // reset the msg count map for next round of auditing

                var auditMsgsAtBatch = self._generateAuditMsgs(self.auditTierAtBatch, self.auditDatacenter,
                    self.topicToMsgcntMapsAtBatch);
                for (var j = 0; j < auditMsgsAtBatch.length; j++) {
                    var auditMsgAtBatch = auditMsgsAtBatch[j];
                    self.produce(self.auditTopicNameC3, auditMsgAtBatch, (Date.now() / 1000));
                }
                self.topicToMsgcntMapsAtBatch = {}; // reset the msg count map for next round of auditing
            };

            self.auditIntervalC3 = setInterval(produceC3AuditMsg, self.auditTimeBucketIntervalInSec * 1000);
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

KafkaBaseProducer.prototype.connect = function connect(onConnect) {
    var self = this;
    if (self.init) {
        var restClientOptions = {
            clientType: self.clientType,
            proxyHost: self.proxyHost,
            proxyPort: self.proxyPort,
            localAgentPort: self.localAgentPort,
            localAgentMode: self.localAgentMode,
            refreshTime: self.proxyRefreshTime,
            produceInterval: self.produceInterval,
            maxRetries: self.maxRetries,
            timeout: self.timeout,
            maxPending: self.maxPending,
            statsd: self.statsd,
            topicLevelOpts: self.topicLevelOpts
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

// Check whether or not we should batch this topic
KafkaBaseProducer.prototype.shouldBatch = function shouldBatch(topic) {
    if (this.batchingWhitelist) {
        return this.batching && this.batchingWhitelist.indexOf(topic) !== -1;
    }
    return this.batching && this.batchingBlacklist.indexOf(topic) === -1;
};

// timeStamp is in second
KafkaBaseProducer.prototype.produce = function produce(topic, message, timeStamp, callback) {
    var self = this;

    if (typeof timeStamp === 'function') {
        callback = timeStamp;
        timeStamp = null;
    }

    if (self.closing) {
        if (callback) {
            callback(new Error('cannot produce to closed KafkaRestProducer'));
        } // TODO: else { self.logger.error() }
        return;
    }

    if (self.restClient) {
        if (self.shouldBatch(topic)) {
            self.batch(topic, message, timeStamp, callback);
        } else {
            var produceMessage = self.getProduceMessage(topic, '', message, timeStamp, 'binary');
            self._produce(produceMessage, callback);
        }

        if (self.enableAudit && topic !== self.auditTopicName) {
            var timeBeginInSec = self._getTimeBeginInSec(Date.now() / 1000);
            self._auditNewMsg(topic, timeBeginInSec, 1, self.topicToMsgcntMaps);
        }

        var isHpTopic = self.restClient.isHeatPipeTopic(topic);
        if (self.enableAuditC3 && topic !== self.auditTopicNameC3 && isHpTopic) {
            var timeBeginInSecC3 = self._getTimeBeginInSecFromHp(message);
            self._auditNewMsg(topic, timeBeginInSecC3, 1, self.topicToMsgcntMapsAtProduce);
        }

    } else if (callback) {
        callback(new Error('Kafka Rest Client is not initialized!'));
    }
};

KafkaBaseProducer.prototype.produceSync = function produce(topic, producerRecord, callback) {
    var self = this;

    if (self.closing) {
        if (callback) {
            callback(new Error('cannot produceSync to closed KafkaDataProducer'));
        }
        return;
    }

    var err = producerRecord.validate();
    if (err !== null) {
        if (callback) {
            callback(err);
        }
        return;
    }

    if (self.restClient) {
        var produceMessage = self.getProduceMessage(topic, producerRecord.key,
            producerRecord.value, Date.now() / 1000, 'binary');
        self._produce(produceMessage, callback);

        if (self.restClient.isHeatPipeTopic(topic) && self.enableAuditC3 && topic !== self.auditTopicNameC3) {
            var timeBeginInSecC3 = self._getTimeBeginInSecFromHp(producerRecord.value);
            self._auditNewMsg(topic, timeBeginInSecC3, 1, self.topicToMsgcntMapsAtProduce);
        }
    } else if (callback) {
        callback(new Error('Kafka Rest Client is not initialized!'));
    }
};

KafkaBaseProducer.prototype._produce = function _produce(msg, callback) {
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
// If something other than a Buffer or string is passed as the message argument,
// this will fail silently unless callback is provided.
KafkaBaseProducer.prototype.batch = function batch(topic, message, timeStamp, callback) {
    var self = this;

    if (self.closing) {
        if (callback) {
            callback(new Error('cannot batch() to closed KafkaRestProducer'));
        } // TODO: else { self.logger.error() }
        return;
    }

    if (typeof message !== 'string' && !Buffer.isBuffer(message)) {
        if (callback) {
            callback(new Error('For batching, message must be a string or buffer, not ' + (typeof message)));
        } // TODO:  else { self.logger.error(); }
        return;
    }

    var messageBatch;

    if (!self.topicToBatchQueue[topic]) {
        messageBatch = new MessageBatch(self.maxBatchSizeBytes);
        self.topicToBatchQueue[topic] = messageBatch;
    } else {
        messageBatch = self.topicToBatchQueue[topic];
    }

    if (messageBatch.exceedsBatchSizeBytes(message)) {
        // Do not add to buffer if message is larger than max buffer size, produce directly
        var produceMessageNonBatch = self.getProduceMessage(topic, '', message, timeStamp, 'binary');
        if (self.enableAuditC3 && topic !== self.auditTopicNameC3) {
            self._auditNewMsg(topic, self._getTimeBeginInSec(produceMessageNonBatch.timeStamp), 1,
                self.topicToMsgcntMapsAtBatch);
        }
        self._produce(produceMessageNonBatch, callback);
    } else {
        if (!messageBatch.canAddMessageToBatch(message)) {
            // Batch is cannot accept this new message, so we should flush to kafka before we add.
            self._produceBatch(messageBatch, topic, timeStamp, callback);
        }

        self.topicToBatchQueue[topic].addMessage(message, callback);
    }
};

KafkaBaseProducer.prototype._produceBatch = function _produceBatch(messageBatch, topic, timeStamp, callback) {
    var self = this;

    var msgCount = messageBatch.getNumMessages();
    var msg = messageBatch.getBatchedMessage();
    var pendingCallbacks = messageBatch.getPendingCallbacks();
    messageBatch.resetBatchedMessage();
    var pmsg = self.getProduceMessage(topic, '', msg, timeStamp, 'batch');

    if (self.enableAuditC3 && topic !== self.auditTopicNameC3) {
        self._auditNewMsg(topic, self._getTimeBeginInSec(pmsg.timeStamp), msgCount, self.topicToMsgcntMapsAtBatch);
    }

    self._produce(pmsg, onProduced);

    function onProduced(err, res) {
        for (var i = 0; i < pendingCallbacks.length; i++) {
            pendingCallbacks[i](err, res);
        }

        if (callback) {
            callback(err, res);
        }
    }
};

KafkaBaseProducer.prototype.flushEntireCache = function flushEntireCache(callback) {
    var self = this;

    var pending = 0;
    var keys = Object.keys(self.topicToBatchQueue);
    for (var i = 0; i < keys.length; i++) {
        var topic = keys[i];
        var messageBatch = self.topicToBatchQueue[topic];

        if (messageBatch.numMessages > 0) {
            var timeStamp = Date.now() / 1000;
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

KafkaBaseProducer.prototype._auditNewMsg = function _auditNewMsg(topic, timeBeginInSec, msgCount, topicToMsgcntMaps) {
    if (msgCount > 0) {
        if (!topicToMsgcntMaps[timeBeginInSec]) {
            topicToMsgcntMaps[timeBeginInSec] = {};
        }
        if (!topicToMsgcntMaps[timeBeginInSec][topic]) {
            topicToMsgcntMaps[timeBeginInSec][topic] = msgCount;
        } else {
            topicToMsgcntMaps[timeBeginInSec][topic] += msgCount;
        }
    }
};

KafkaBaseProducer.prototype._getTimeBeginInSec = function _getTimeBeginInSec(nowInSec) {
    var self = this;

    // sanity check in case timestamp is in ms
    // 999999999999 (12 digits), as millisecond, means Sun Sep 09 2001 01:46:39 UTC.
    // 1000000000000 (13 digits), as second, means Fri Sep 27 33658 01:46:40....
    if (nowInSec > 999999999999.0) {
        return Math.floor((nowInSec / 1000) / self.auditTimeBucketIntervalInSec) * self.auditTimeBucketIntervalInSec;
    } else {
        return Math.floor(nowInSec / self.auditTimeBucketIntervalInSec) * self.auditTimeBucketIntervalInSec;
    }
};

KafkaBaseProducer.prototype._getTimeBeginInSecFromHp = function _getTimeBeginInSecFromHp(message) {
    var self = this;
    var nowInSec = Date.now() / 1000;
    if (Buffer.isBuffer(message) && message.length > self.hpMsgMinLength) {
        var tsInSec = message.readDoubleLE(self.hpMsgTsOffset);
        if (tsInSec >= (nowInSec - self.timestampSkewLimitInSec) &&
            tsInSec <= (nowInSec + self.timestampSkewLimitInSec)) {
            return Math.floor(tsInSec / self.auditTimeBucketIntervalInSec) * self.auditTimeBucketIntervalInSec;
        }
    }
    return Math.floor(nowInSec / self.auditTimeBucketIntervalInSec) * self.auditTimeBucketIntervalInSec;
};

KafkaBaseProducer.prototype._generateAuditMsgs = function _generateAuditMsgs(auditTier, auditDatacenter,
                                                                             topicToMsgcntMaps) {
    var self = this;
    var auditMsgs = [];
    var keys = Object.keys(topicToMsgcntMaps);
    for (var i = 0; i < keys.length; i++) {
        var timeBeginInSec = keys[i];
        var auditMsg = self._generateAuditMsg(parseInt(timeBeginInSec, 10), auditTier, auditDatacenter,
            topicToMsgcntMaps[timeBeginInSec]);
        if (auditMsg) {
            auditMsgs.push(auditMsg);
        }
    }
    return auditMsgs;
};

KafkaBaseProducer.prototype._generateAuditMsg = function _generateAuditMsg(timeBeginInSec, auditTier, auditDatacenter,
                                                                           topicToMsgcntMap) {
    var self = this;
    var keys = Object.keys(topicToMsgcntMap);
    if (keys.length > 0) {
        /* eslint-disable camelcase */
        /* jshint camelcase: false */
        var auditMsg = {
            time_bucket_start: timeBeginInSec,
            time_bucket_end: timeBeginInSec + self.auditTimeBucketIntervalInSec,
            tier: auditTier,
            hostname: hostName,
            datacenter: auditDatacenter,
            version: KafkaVersion,
            topic_count: {}
        };

        for (var i = 0; i < keys.length; i++) {
            var topic = keys[i];
            auditMsg.topic_count[topic] = topicToMsgcntMap[topic];
        }
        /* jshint camelcase: true */
        /* eslint-enable camelcase */
        return JSON.stringify(auditMsg);
    }
    return null;
};

/* jshint maxparams: 5 */
KafkaBaseProducer.prototype.getProduceMessage = function getProduceMessage(topic, key, message, timeStamp, type) {
    var produceMessage = {};
    produceMessage.topic = topic;
    if (key !== undefined && key !== '') {
        produceMessage.key = key;
    }
    produceMessage.message = message;
    produceMessage.timeStamp = timeStamp || (Date.now() / 1000);
    produceMessage.type = type;
    return produceMessage;
};

KafkaBaseProducer.prototype.logLine = function logLine(topic, message, callback) {
    var self = this;
    var timeStamp = Date.now() / 1000.0;
    self.logLineWithTimeStamp(topic, message, timeStamp, callback);
};

KafkaBaseProducer.prototype.logLineWithTimeStamp = function logLine(topic, message, timeStamp, callback) {
    var self = this;
    var wholeMessage = JSON.stringify(self.getWholeMsg(topic, message, timeStamp));
    self.produce(topic, wholeMessage, timeStamp, function handleResponse(err, res) {
        if (callback) {
            callback(err, res);
        }
    });
};

// log_line has always been the standard function name, this actually causes compatibility problems
/* eslint-disable camelcase */
/* jshint camelcase: false */
KafkaBaseProducer.prototype.log_line = KafkaBaseProducer.prototype.logLine;
/* eslint-enable camelcase */
/* jshint camelcase: true */

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

KafkaBaseProducer.prototype.getWholeMsg = function getWholeMsg(topic, message, timeStamp) {
    var self = this;
    if (self.shouldAddTopicToMessage) {
        return new ProducedTopicMessage(message, timeStamp, hostName, topic);
    }
    return new ProducedMessage(message, timeStamp, hostName);
};

KafkaBaseProducer.prototype.close = function close(callback) {
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

    if (self.auditInterval) {
        clearInterval(self.auditInterval);
        var auditMsgs = self._generateAuditMsgs(self.auditTier, self.auditDatacenter, self.topicToMsgcntMaps);
        for (var i = 0; i < auditMsgs.length; i++) {
            var produceMessage = self.getProduceMessage(self.auditTopicName, '', auditMsgs[i],
                (Date.now() / 1000), 'binary');
            self._produce(produceMessage);
        }
        self.topicToMsgcntMaps = {}; // reset the msg count map for next round of auditing
    }

    if (self.auditIntervalC3) {
        clearInterval(self.auditIntervalC3);
        var auditMsgsAtProduce = self._generateAuditMsgs(self.auditTierAtProduce, self.auditDatacenter,
            self.topicToMsgcntMapsAtProduce);
        for (var j = 0; j < auditMsgsAtProduce.length; j++) {
            var produceMessageAtProduce = self.getProduceMessage(self.auditTopicNameC3, '', auditMsgsAtProduce[j],
                (Date.now() / 1000), 'binary');
            self._produce(produceMessageAtProduce);
        }
        self.topicToMsgcntMapsAtProduce = {}; // reset the msg count map for next round of auditing

        var auditMsgsAtBatch = self._generateAuditMsgs(self.auditTierAtBatch, self.auditDatacenter,
            self.topicToMsgcntMapsAtBatch);
        for (var k = 0; k < auditMsgsAtBatch.length; k++) {
            var produceMessageAtBatch = self.getProduceMessage(self.auditTopicNameC3, '', auditMsgsAtBatch[k],
                (Date.now() / 1000), 'binary');
            self._produce(produceMessageAtBatch);
        }
        self.topicToMsgcntMapsAtBatch = {}; // reset the msg count map for next round of auditing
    }

    self._tryClose();
};

KafkaBaseProducer.prototype._tryClose = function _tryClose() {
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

module.exports = KafkaBaseProducer;
