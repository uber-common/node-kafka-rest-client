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

var hostName = require('os').hostname();
var KafkaRestClient = require('./kafka_rest_client');

function KafkaProducer(options, callback) {
    // Trying to init KafkaProducer
    var self = this;
    self.proxyHost = options.proxyHost || 'localhost';
    // proxyPort is must have, otherwise KafkaProducer is disabled.
    if ('proxyPort' in options) {
        self.proxyPort = options.proxyPort;
        if ('proxyRefreshTime' in options) {
            self.proxyRefreshTime = options.proxyRefreshTime;
        } else {
            // default refresh time is 30 min.
            self.proxyRefreshTime = 1000 * 60 * 30;
        }
        if ('shouldAddTopicToMessage' in options) {
            self.shouldAddTopicToMessage = options.shouldAddTopicToMessage;
        } else {
            self.shouldAddTopicToMessage = false;
        }
        self.restClient = new KafkaRestClient(self.proxyHost, self.proxyPort, self.proxyRefreshTime);
        self.enable = true;
    } else {
        self.enable = false;
    }

}

KafkaProducer.prototype.produce = function produce(topic, message, callback) {
    var self = this;

    if (typeof message !== 'string') {
        message = JSON.stringify(message);
    }
    self.restClient.produce(topic, message, 'binary', function handleResponse(err, res) {
        if (callback) {
            callback(err, res);
        }
    });

};

KafkaProducer.prototype.logLine = function logLine(topic, message, callback) {
    var self = this;
    var wholeMessage = self.getWholeMsg(topic, message);
    self.produce(topic, wholeMessage, function handleResponse(err, res) {
        if (callback) {
            callback(err, res);
        }
    });
};

function ProducedMessage(msg, ts, host) {
    var message = {
        ts: ts,
        host: host,
        msg: msg
    };
    return message;
}

function ProducedTopicMessage(msg, ts, host, topic) {
    var message = {
        ts: ts,
        host: host,
        msg: msg,
        topic: topic
    };
    return message;
}

KafkaProducer.prototype.getWholeMsg = function getWholeMsg(topic, message) {
    var self = this;
    if (self.shouldAddTopicToMessage) {
        return new ProducedTopicMessage(message, Date.now() / 1000.0, hostName, topic);
    }
    return new ProducedMessage(message, Date.now() / 1000.0, hostName);
};

KafkaProducer.prototype.close = function close(callback) {
    var self = this;
    self.restClient.close();
};

module.exports = KafkaProducer;
