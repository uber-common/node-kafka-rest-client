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
var Buffers = require('buffers');
var KafkaRestClientFactory = require('./kafka_rest_client_factory');

function KafkaSingleTopicProducer(clientFactory, topic, batching, maxBatchSizeBytes, callback) {
    var self = this;

    self.topic = topic;
    self.clientFactory = clientFactory;
    self.flushCycleSecs = 1;
    self.lastFlush = new Date().getTime();
    self.flushQueue = clientFactory.queue;
    self.maxMessageSizeBytes = 1000100;
    self.maxBatchSizeBytes = maxBatchSizeBytes;
    self.messageBatch = new MessageBatch();
    self.batching = batching;

    self.kafkaRestClient = null;
    try {
        self.kafkaRestClient = clientFactory.getKafkaRestClient(topic);
    } catch(e) {
        callback(e);
    }
}

KafkaSingleTopicProducer.prototype.produce = function produce(message, callback) {
    var self = this;

    if (message.length > self.maxMessageSizeBytes) {
        callback(new Error('Got a message bigger than Max allowed size of 1MB. Message Dropped!'));
        return;
    }

    if (self.batching) {
        self.batch(message);
        callback(); // todo confirm
    } else {
        self.kafkaRestClient.produce(message, callback);
    }
};

KafkaSingleTopicProducer.prototype.batch = function batch(message, callback) {
    var self = this;

    var currTime = new Date().getTime();
    var msg = message.message;

    if ((self.messageBatch.sizeBytes + msg.length) > self.maxBatchSizeBytes ||
        (currTime - self.lastFlush) > self.flushCycleSecs * 1000) {
        self.flushToQueue(callback);
    }

    self.messageBatch.addMessage(msg);
};

KafkaSingleTopicProducer.prototype.flushToQueue = function flushMessageBatch(callback) {
    var self = this;

    try {
        if (self.messageBatch.numMessages > 0) {
            self.flushQueue.addBatchedMessage(self.messageBatch);
            self.messageBatch = new MessageBatch(self.topic);
            self.lastFlush = new Date().getTime();
        }
    } catch(err) {
        callback(err);
    }
};

KafkaSingleTopicProducer.prototype.logLine = function logLine(message, callback) {
    var self = this;
    // todo check
    self.kafka_client.produce(message);
};

KafkaSingleTopicProducer.getKafkaSingleTopicProducer = function getKafkaSingleTopicProducer(topic,
    batching,
    maxBatchSizeBytes,
    options,
    callback) {
    var clientFactory = KafkaRestClientFactory.getKafkaRestClientFactory(options, callback);
    return new KafkaSingleTopicProducer(clientFactory, topic, batching, maxBatchSizeBytes);
};

// The batch message payload should follow:
// 4 BE bytes number of messages + 4 BE bytes size of message + actual message
function MessageBatch(topic) {
    var self = this;

    self.payload = Buffers();
    self.timestamp = new Date().getTime();
    self.numMessages = 0;
    self.sizeBytes = 0;
    self.topic = topic;
}

MessageBatch.prototype.addMessage = function addMessage(message) {
    var self = this;
    // todo work for heatpipe?
    var messageSize = message.length;
    var sizeBuf = new Buffer(4);
    sizeBuf.writeFloatBE(messageSize, 0);

    self.payload.push(sizeBuf, new Buffer(message, 'ascii'));
    self.numMessages += 1;
    self.sizeBytes += messageSize;
};

MessageBatch.prototype.getBatchedMessage = function getBatchedMessage() {
    var self = this;

    var sizeBuf = new Buffer(4);
    sizeBuf.writeFloatBE(self.numMessages, 0);

    var message = Buffers([
        sizeBuf,
        self.payload.toBuffer()
    ]).toBuffer();

    return message;
};

module.exports = KafkaSingleTopicProducer;
