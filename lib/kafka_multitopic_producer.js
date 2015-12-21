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

    self.proxyHost = options.proxyHost || 'localhost';
    self.maxRetries = options.maxRetries || 1;

    if ('proxyPort' in options) {

        self.proxyPort = options.proxyPort;

        if ('proxyRefreshTime' in options) {
            self.proxyRefreshTime = options.proxyRefreshTime;
        } else {
            self.proxyRefreshTime = 1000 * 60 * 30;
        }

        self.blacklistMigratorHttpClient = null;
        if ('blacklistMigrator' in options && 'blacklistMigratorUrl' in options) {
            if (options.blacklistMigrator) {
                self.blacklistMigratorHttpClient = new MigratorBlacklistClient(options.blacklistMigratorUrl);
            }
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

        self.init = true;
    } else {
        self.init = false;
        self.enable = false;
    }
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