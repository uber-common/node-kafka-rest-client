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
var KafkaProducer = require('../lib/kafka_producer');

var configs = {
    proxyHost: 'localhost',
    proxyPort: 8080,
    proxyRefreshTime: 0
};

function onConnect(err) {
    /* eslint-disable no-undef,no-console,block-scoped-var */
    if (!err) {
        console.log('KafkaRestClient connected to kafka');
    } else {
        console.log('KafkaRestClient could not connect to kafka');
    }
    /* eslint-enable no-undef,no-console,block-scoped-var */
}

var kafkaProducer = new KafkaProducer(configs);
kafkaProducer.connect(onConnect);

/* eslint-disable no-undef,no-console,block-scoped-var */
function msgCallback(err, res) {
    if (err) {
        console.log(err);
    } else {
        console.log(res);
    }
}
/* eslint-enable no-undef,no-console,block-scoped-var */

// Timestamped log entry with host
kafkaProducer.logLine('test', 'Important message', msgCallback);
// Just dump log, no callback.
kafkaProducer.logLine('test', 'Important message #1');
// Just dump log, no callback.
kafkaProducer.logLineWithTimeStamp('test', 'Important message #1.5', 1.5);
// Just sends the raw message directly to kafka
kafkaProducer.produce('test', 'Important message #2', 'ts2', msgCallback);
// Just sends the raw message directly to kafka
kafkaProducer.produce('test', 'Important message #2.5', 'ts2.5', msgCallback);
/* eslint-disable no-undef,block-scoped-var */
setTimeout(function produce3() {
    kafkaProducer.produce('test', 'Important message #3', 'ts3', msgCallback);
}, 2000);

setTimeout(function produce4() {
    kafkaProducer.logLine('test', 'Important message #4', msgCallback);
}, 2000);

setTimeout(function close() {
    kafkaProducer.close();
}, 3000);
/* eslint-enable no-undef,block-scoped-var */
