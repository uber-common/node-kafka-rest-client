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
var Buffer = require('buffer').Buffer;

var maxBatchSizeBytes = 100000;

test('MessageBatch can batch several strings', function testMessageBatchString(assert) { // eslint-disable-line
    var messageBatch = new MessageBatch(maxBatchSizeBytes);
    messageBatch.addMessage('This is a test.');
    messageBatch.addMessage('Foo');
    messageBatch.addMessage('Bar');
    var batchedMessage = messageBatch.getBatchedMessage();
    var numMessages = batchedMessage.readInt32BE(0);
    var firstMessageSize = batchedMessage.readInt32BE(4);
    var secondMessageSize = batchedMessage.readInt32BE(23);
    var thirdMessageSize = batchedMessage.readInt32BE(30);

    assert.equal(messageBatch.numMessages, 3);
    assert.equal(messageBatch.sizeBytes, 37);
    assert.equal(numMessages, 3);
    assert.equal(firstMessageSize, 15);
    assert.equal(secondMessageSize, 3);
    assert.equal(thirdMessageSize, 3);
    assert.equal(batchedMessage.toString(undefined, 8, 23), 'This is a test.');
    assert.equal(batchedMessage.toString(undefined, 27, 30), 'Foo');
    assert.equal(batchedMessage.toString(undefined, 34, 37), 'Bar');

    messageBatch.resetBatchedMessage();
    messageBatch.addMessage('This is a test.');
    messageBatch.addMessage('Foo');
    messageBatch.addMessage('Bar');
    batchedMessage = messageBatch.getBatchedMessage();
    numMessages = batchedMessage.readInt32BE(0);

    assert.equal(messageBatch.numMessages, 3);
    assert.equal(messageBatch.sizeBytes, 37);
    assert.equal(numMessages, 3);
    assert.equal(firstMessageSize, 15);
    assert.equal(secondMessageSize, 3);
    assert.equal(thirdMessageSize, 3);
    assert.equal(batchedMessage.toString(undefined, 8, 23), 'This is a test.');
    assert.equal(batchedMessage.toString(undefined, 27, 30), 'Foo');
    assert.equal(batchedMessage.toString(undefined, 34, 37), 'Bar');
    assert.end();
});

test('MessageBatch can batch several buffers', function testMessageBatchBuffers(assert) { // eslint-disable-line
    var messageBatch = new MessageBatch(maxBatchSizeBytes);
    messageBatch.addMessage(new Buffer('This is a test.'));
    messageBatch.addMessage(new Buffer('Foo'));
    messageBatch.addMessage(new Buffer('FooBar'));
    var batchedMessage = messageBatch.getBatchedMessage();
    var numMessages = batchedMessage.readInt32BE(0);
    var firstMessageSize = batchedMessage.readInt32BE(4);
    var secondMessageSize = batchedMessage.readInt32BE(23);
    var thirdMessageSize = batchedMessage.readInt32BE(30);

    assert.equal(messageBatch.numMessages, 3);
    assert.equal(messageBatch.sizeBytes, 40);
    assert.equal(numMessages, 3);
    assert.equal(firstMessageSize, 15);
    assert.equal(secondMessageSize, 3);
    assert.equal(thirdMessageSize, 6);
    assert.equal(batchedMessage.toString(undefined, 8, 23), 'This is a test.');
    assert.equal(batchedMessage.toString(undefined, 27, 30), 'Foo');
    assert.equal(batchedMessage.toString(undefined, 34, 40), 'FooBar');

    messageBatch.resetBatchedMessage();
    messageBatch.addMessage(new Buffer('This is a test.'));
    messageBatch.addMessage(new Buffer('Foo'));
    messageBatch.addMessage(new Buffer('FooBar'));
    batchedMessage = messageBatch.getBatchedMessage();
    numMessages = batchedMessage.readInt32BE(0);

    assert.equal(messageBatch.numMessages, 3);
    assert.equal(messageBatch.sizeBytes, 40);
    assert.equal(numMessages, 3);
    assert.equal(firstMessageSize, 15);
    assert.equal(secondMessageSize, 3);
    assert.equal(thirdMessageSize, 6);
    assert.equal(batchedMessage.toString(undefined, 8, 23), 'This is a test.');
    assert.equal(batchedMessage.toString(undefined, 27, 30), 'Foo');
    assert.equal(batchedMessage.toString(undefined, 34, 40), 'FooBar');
    assert.end();
});
