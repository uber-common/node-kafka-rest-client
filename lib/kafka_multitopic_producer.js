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

var KafkaSingleTopicProducer = require('./kafka_singletopic_producer');
var KafkaRestClientFactory = require('./kafka_rest_client_factory');

function KafkaMultiTopicProducer(options, callback) {
    var self = this;

    self.options = options;
    if()
    self.cache = {};

    if('batchOptions' in options) {
        self.batchOptions = options.batchOptions;
    } else {
        self.batchOptions = {
            batching: true,
            batchSizeBytes: 100000,
            flushCycleSecs: 1,
            queueSizeBytes: 50000000
        };
    }

    if(!'proxyPort' in options && !'proxyHost' in options) {
        throw new Error('Must include proxyPort and proxyHost in KafkaMultiTopicProducer options!');
    }

    self.proxyHost = options.proxyHost;
    self.proxyPort = options.proxyPort;

    self.blacklistMigratorHttpClient = false;
    if ('blacklistMigrator' in options && 'blacklistMigratorUrl' in options) {
        if (options.blacklistMigrator) {
            self.blacklistMigratorHttpClient = new MigratorBlacklistClient(options.blacklistMigratorUrl);
        }
    }

    self.shouldAddTopicToMessage = false;
    if ('shouldAddTopicToMessage' in options) {
        self.shouldAddTopicToMessage = options.shouldAddTopicToMessage;
    }

    self.statsd = false;
    if ('statsd' in options) {
        self.statsd = options.statsd;
    }

    var kafkaRestClientFactoryOptions = {};
    kafkaRestClientFactoryOptions.proxyHost = self.proxyHost;
    kafkaRestClientFactoryOptions.proxyPort = self.proxyPort;

    self.clientFactory = KafkaRestClientFactory.getKafkaRestClientFactory(kafkaRestClientFactoryOptions);
};

KafkaProducer.prototype.connect = function connect(onConnect) {
    var self = this;
};

KafkaProducer.prototype.produce = function produce(topic, message, timeStamp, callback) {
    var self = this;
};

KafkaProducer.prototype.logLine = function logLine(topic, message, callback) {
    var self = this;
};

KafkaProducer.prototype.close = function close(callback) {
    var self = this;
};

module.exports = KafkaMultiTopicProducer;