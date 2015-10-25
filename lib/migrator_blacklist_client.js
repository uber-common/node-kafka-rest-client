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

// Kafka 7 to 8 Migrator Blacklist Client.
// Initialization: takes migrator url to init http client
// Blacklist: for blacklist request, http client will send a POST call to
// path: /topics/[topicName]
function MigratorBlacklistClient(blacklistMigratorUrl) {
    var self = this;
    self.blacklistMigratorUrl = blacklistMigratorUrl;
    self.enable = false;
    self.alreadyBlacklistTopics = [];
    self.alreadyQueriedBlacklistTopics = [];
    if (self.blacklistMigratorUrl) {
        self.blacklistMigratorHttpClient = new lbPool.Pool(http, [self.blacklistMigratorUrl]);
        self.enable = true;
    } else {
        self.blacklistMigratorHttpClient = null;
    }
}

MigratorBlacklistClient.prototype.blacklistTopic = function blacklistTopic(topic, callback) {
    var self = this;
    if (self.alreadyQueriedBlacklistTopics.indexOf(topic) > -1) {
        if (self.alreadyBlacklistTopics.indexOf(topic) > -1) {
            callback(true);
        } else {
            callback(false);
        }
    } else {
        self.alreadyQueriedBlacklistTopics.push(topic);
        var reqOpts = url.parse('http://' + self.blacklistMigratorUrl);
        reqOpts.method = 'POST';
        reqOpts.path = '/topics/' + topic;
        reqOpts.headers = {};
        self.blacklistMigratorHttpClient.post(reqOpts, null, function handlePostCall(err, res, body) {
            var isBlacklistSucceed = false;
            if (err) {
                isBlacklistSucceed = false;
            }
            if (res) {
                if (res.statusCode === 404 || res.statusCode === 200) {
                    isBlacklistSucceed = true;
                }
            }
            if (isBlacklistSucceed) {
                self.alreadyBlacklistTopics.push(topic);
            }
            callback(isBlacklistSucceed);
        });
    }
};

MigratorBlacklistClient.prototype.close = function close() {
    // Trying to close kafka rest client, need to cancel timer scheduler.
    var self = this;
    self.enable = false;
    if (self.blacklistMigratorHttpClient) {
        self.blacklistMigratorHttpClient.close();
    }
};

module.exports = MigratorBlacklistClient;
