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
var KafkaRestClient = require('./kafka_rest_client');

var instance = null;

function KafkaRestClientFactory(options, callback) {
    var self = this;

    self.options;

    self.proxyHost = options.proxyHost;
    self.proxyPort = options.proxyPort;

    # map topic to address
    self.topicAddressMap = {};

    # map one KafkaRestClient per address
    self.addressClientMap = {};

    self.loadTopicMap(callback);
};

KafkaRestClientFactory.prototype.getKafkaRestClientFactory = function getKafkaRestClientFactory(options) {
    if(!instance) {
        instance = new KafkaRestClientFactory(options);
    }

    return instance;
};

function formatTopicAddressMapJSON(body) {
    var self = this;
    try {
        var urlToTopicsJson = JSON.parse(body);
        var topicToUrlMapping = {};
        var urlArray = Object.keys(urlToTopicsJson);
        for (var i = 0; i < urlArray.length; i++) {
            var reqUrl = urlArray[i];
            var topicsArray = urlToTopicsJson[reqUrl];
            for (var j = 0; j < topicsArray.length; j++) {
                topicToUrlMapping[topicsArray[j]] = reqUrl;
            }
        }

        return topicToUrlMapping;
    } catch(e) {
        return self.topicAddressMap;
    }
};

KafkaRestClientFactory.prototype.loadTopicMap = function loadTopicMap(callback) {
    KafkaRestClient.getAllTopicsRequest(self.proxyHost, self.proxyPort, function updateTopicAddressMap(err, res) {
        if (err) {
            callback(err);
        } else {
            var topicAddressMap = formatTopicAddressMapJSON(res);

            if(Object.keys(topicAddressMap).length > 0) {
                self.topicAddressMap = formatTopicAddressMapJSON(res);
                //todo makes Address Client Map
            }
        }
    });
};

module.exports = KafkaRestClientFactory;