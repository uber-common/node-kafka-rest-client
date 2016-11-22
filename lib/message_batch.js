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

// The batch message payload should follow:
// 4 BE bytes number of messages + 4 BE bytes size of message + actual message
var KAFKA_PADDINGS = {
  MESSAGE_PADDING_BYTES: 4,
  BATCH_PADDING_BYTES: 4
};
Object.freeze(KAFKA_PADDINGS);

function MessageBatch(size) {
    var self = this;

    self.maxBufferSize = size;
    self.cachedBuf = new Buffer(self.maxBufferSize);
    self.currOffset = KAFKA_PADDINGS.BATCH_PADDING_BYTES;
    self.timestamp = new Date().getTime();
    self.numMessages = 0;
    self.sizeBytes = KAFKA_PADDINGS.BATCH_PADDING_BYTES;
    self.pendingCallbacks = [];
}

MessageBatch.prototype.exceedsBatchSizeBytes = function exceedsBatchSizeBytes(message) {
    var self = this;
    // No single message can be a batch by itself unless it is smaller
    // than our buffer size
    return message.length + KAFKA_PADDINGS.BATCH_PADDING_BYTES +
        KAFKA_PADDINGS.MESSAGE_PADDING_BYTES > self.maxBufferSize;
};

MessageBatch.prototype.canAddMessageToBatch = function canAddMessageToBatch(message) {
    var self = this;
    return self.currOffset + KAFKA_PADDINGS.MESSAGE_PADDING_BYTES + message.length < self.maxBufferSize;
};

MessageBatch.prototype.addMessage = function addMessage(message, callback) {
    var self = this;

    var bytesWritten = KAFKA_PADDINGS.MESSAGE_PADDING_BYTES;
    var offset = self.currOffset;
    var msgOffset = offset + KAFKA_PADDINGS.MESSAGE_PADDING_BYTES;

    if (typeof message === 'string') {
        bytesWritten += self.cachedBuf.write(message, msgOffset);
    } else if (Buffer.isBuffer(message)) {
        // byte array message
        message.copy(self.cachedBuf, msgOffset);
        bytesWritten += message.length;
    } else {
        var err = new Error('For batching, message must be a string or buffer, not ' + (typeof message));
        if (callback) {
            callback(err);
        } // TODO: else { self.logger.error(err); }
        return;
    }

    self.cachedBuf.writeInt32BE(bytesWritten - KAFKA_PADDINGS.MESSAGE_PADDING_BYTES, offset);

    self.numMessages += 1;
    self.sizeBytes += bytesWritten;
    self.currOffset += bytesWritten;

    if (callback) {
        self.pendingCallbacks.push(callback);
    }
};

MessageBatch.prototype.getBatchedMessage = function getBatchedMessage() {
    var self = this;

    var currBatchedMessage = new Buffer(self.sizeBytes);
    currBatchedMessage.writeInt32BE(self.numMessages, 0);
    self.cachedBuf.copy(currBatchedMessage, KAFKA_PADDINGS.BATCH_PADDING_BYTES,
        KAFKA_PADDINGS.BATCH_PADDING_BYTES, self.currOffset);

    return currBatchedMessage;
};

MessageBatch.prototype.getNumMessages = function getNumMessages() {
    return this.numMessages;
};

MessageBatch.prototype.getPendingCallbacks = function getPendingCallbacks() {
    return this.pendingCallbacks;
};

MessageBatch.prototype.resetBatchedMessage = function resetBatchedMessage() {
    var self = this;

    self.currOffset = KAFKA_PADDINGS.BATCH_PADDING_BYTES;
    self.numMessages = 0;
    self.sizeBytes = KAFKA_PADDINGS.BATCH_PADDING_BYTES;
    self.pendingCallbacks = [];
};

module.exports = MessageBatch;
