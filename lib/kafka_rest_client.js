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

var url = require('url');
var http = require('http');
var lbPool = require('lb_pool');
var os = require('os');

var supportContentType = {
    'binary': 'application/vnd.kafka.binary.v1',
    'json': 'application/vnd.kafka.json.v1+json',
    'batch': 'application/vnd.kafka.binary.batch.v1'
};

var emptyFunction = function EmptyFunction() {
};

/*
  Kafka 8 Rest Proxy Client.
  Initialization: takes host and port and send a GET call to get all the
  <topic, host:port> mapping.
  Produce: for sending request, forkafka client will send a POST call
*/
function KafkaRestClient(options, callback) {
    var self = this;
    callback = callback || emptyFunction;

    self.proxyHost = options.proxyHost;
    self.proxyPort = options.proxyPort;
    self.requestUrl = self.proxyHost + ':' + self.proxyPort;
    self.topicHttpClient = new lbPool.Pool(http, [self.requestUrl], {
        keep_alive: true // eslint-disable-line
    });

    self.maxRetries = options.maxRetries;

    self.statsd = (options.statsd && typeof options.statsd.increment === 'function') ? options.statsd : null;
    self.metricsPrefix = 'kafka_rest_proxy.rest-client.' + os.hostname() + '.';
}

KafkaRestClient.getTopicRequestBody = function getTopicRequestBody(proxyHost, proxyPort, callback) {
    var self = this;
    var httpClient = new lbPool.Pool(http, [proxyHost + ';' + proxyPort], {
         keep_alive: true // eslint-disable-line
     });
    httpClient.get('/topics', function handleGetCall(err, res, body) {
        if (err) {
            callback(err, null);
        } else {
            callback(null, body);
        }
    });
};

/*
 ProduceMessage {
   string topic,
   binary message,
   string type,
   long timeStamp
 }
*/
KafkaRestClient.prototype.produce = function produce(produceMessage, callback) {
    var self = this;

    self.produceWithRetry(url, produceMessage, 0, callback);
    if (self.statsd) {
        self.statsd.increment(self.metricsPrefix + produceMessage.topic + '.produced');
    }
};

KafkaRestClient.prototype.produceWithRetry = function produceWithRetry(produceMessage, retry, callback) {
    var self = this;

    var reqOpts = url.parse('http://' + pathUrl);
    reqOpts.method = 'POST';
    reqOpts.path = '/topics/' + produceMessage.topic;
    reqOpts.headers = {
        'Content-Type': supportContentType[produceMessage.type],
        'TimeStamp': produceMessage.timeStamp.toString()
    };
    
    var time = new Date();
    self.topicHttpClient.post(reqOpts, produceMessage.message, function handlePostCall(err, res, body) {
        var metricPrefix = self.metricsPrefix + produceMessage.topic;

        if (self.statsd && typeof self.statsd.timing === 'function') {
            self.statsd.timing(metricPrefix + '.latency', time);
        }

        if (err) {
            // Only retry with exception: Connection refused
            if (err.reason === 'connect ECONNREFUSED' && retry < self.maxRetries) {
                // 3sec, 15sec, 75sec retry
                setTimeout(function delayedProduce() { // eslint-disable-line
                    self.produceWithRetry(produceMessage, retry + 1, callback);
                }, 600 * Math.pow(5, retry + 1));

                if (self.statsd) {
                    self.statsd.increment(metricPrefix + '.retry');
                }
            } else {
                callback(err);

                if (self.statsd) {
                    self.statsd.increment(metricPrefix + '.error');
                }
            }
        } else {
            callback(null, body);
        }
    });

};

KafkaRestClient.prototype.close = function close() {
    var self = this;

    self.topicHttpClient.close();
    if (self.blacklistMigratorHttpClient) {
        self.blacklistMigratorHttpClient.close();
    }
};

module.exports = KafkaRestClient;
