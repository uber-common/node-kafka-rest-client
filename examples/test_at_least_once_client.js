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
var KafkaDataProducer = require('../index').KafkaDataProducer;
var ProducerRecord = require('../index').ProducerRecord;

var configs = {
    clientType: 'AtLeastOnce',
    proxyHost: 'localhost',
    proxyPort: 16947
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

var kafkaDataProducer = new KafkaDataProducer(configs);
kafkaDataProducer.connect(onConnect);

/* eslint-disable no-undef,no-console,block-scoped-var */
function msgCallback(err, res) {
    if (err) {
        console.log(err);
    } else {
        console.log(res);
    }
}
/* eslint-enable no-undef,no-console,block-scoped-var */

kafkaDataProducer.produceSync('test_hongxu', new ProducerRecord({value: 'value hahaha'}), msgCallback);
kafkaDataProducer.produceSync('test_hongxu', new ProducerRecord({key: 'test key', value: 'value hahaha'}), msgCallback);

/* eslint-disable no-undef,block-scoped-var */
setTimeout(function close() {
    kafkaDataProducer.close();
}, 1000);
/* eslint-enable no-undef,block-scoped-var */
