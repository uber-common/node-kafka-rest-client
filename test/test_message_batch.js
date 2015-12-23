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

var test = require('tape');
var MessageBatch = require('../lib/message_batch');

var maxBatchSizeBytes = 100000;

test('MessageBatch can batch several strings', function testKafkaRestClientTopicDiscovery(assert) {
    var messageBatch = new MessageBatch(maxBatchSizeBytes);
    messageBatch.addMessage('This is a test.');
    messageBatch.addMessage('Foo');
    messageBatch.addMessage('Bar');
    var batchedMessage = messageBatch.getBatchedMessage();
    var numMessages = batchedMessage.readInt32BE(0);

    assert.equal(messageBatch.numMessages, 3);
    assert.equal(messageBatch.sizeBytes, 37);
    assert.equal(numMessages, 3);
    assert.end();
});
