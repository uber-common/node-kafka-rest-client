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
function MessageBatch(size) {
    var self = this;

    self.cachedBuf = new Buffer(size);
    self.currOffset = 4;
    self.timestamp = new Date().getTime();
    self.numMessages = 0;
    self.sizeBytes = 4;
}

MessageBatch.prototype.addMessage = function addMessage(message, callback) {
    var self = this;

    var bytesWritten = 4;
    var offset = self.currOffset;
    var msgOffset = offset + 4;

    self.cachedBuf.writeInt32BE(bytesWritten, offset);

    if(typeof message == 'string') {
        bytesWritten += self.cachedBuf.write(message, msgOffset);
    } else if (Buffer.isBuffer(message)){
        // byte array message
        message.copy(self.cachedBuf, msgOffset);
        bytesWritten += message.length;
    } else {
        callback(new Error('For batching, message must be a string or buffer!'));
    }

    self.numMessages += 1;
    self.sizeBytes += bytesWritten;
    self.currOffset += 4 + bytesWritten;
};

MessageBatch.prototype.getBatchedMessage = function getBatchedMessage() {
    var self = this;

    self.cachedBuf.writeUInt32BE(self.numMessages, 0);

    return self.cachedBuf.slice(0, self.currOffset);
};

module.exports = MessageBatch;
