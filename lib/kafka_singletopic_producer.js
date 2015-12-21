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
//var KafkaRestClientFactory = require('./kafka_rest_client_factory');

function KafkaSingleTopicProducer(options, callback) {

};

function MessageBatch() {
    var self = this;

    self.payload = Buffers();
    self.timestamp = new Date().getTime();
    self.numMessages = 0;
    self.sizeBytes = 0;
};

MessageBatch.prototype.addMessage= function addMessage(message) {
    var self = this;

    var messageSize = Buffer.byteLength(message, 'ascii');
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