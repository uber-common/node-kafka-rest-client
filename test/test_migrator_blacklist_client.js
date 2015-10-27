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

var MigratorBlacklistServer = require('./lib/test_migrator_blacklist_server');
var MigratorBlacklistClient = require('../lib/migrator_blacklist_client');

test('MigratorBlacklistClient returns false if port is not available', function testMigratorBlacklistClient(assert) {
    var migratorBlacklistClient = new MigratorBlacklistClient('localhost:11111');
    migratorBlacklistClient.blacklistTopic('testTopic', function assertError(isDone) {
        assert.equal(isDone, false);
    });
    migratorBlacklistClient.close();
    assert.end();
});

test('MigratorBlacklistClient can return ', function testMigratorBlacklistClient(assert) {
    var server = new MigratorBlacklistServer(2222);
    server.start();
    var migratorBlacklistClient = new MigratorBlacklistClient('localhost:2222');
    assert.equal(migratorBlacklistClient.alreadyQueriedTopics.length, 0);
    assert.equal(migratorBlacklistClient.alreadyBlacklistedTopics.length, 0);
    assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic0') === -1,
        'testTopic0 not in alreadyQueriedTopics');
    assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic0') === -1,
        'testTopic0 not in alreadyBlacklistedTopics');
    migratorBlacklistClient.blacklistTopic('testTopic0', function assertDone(isDone) {
        assert.equal(isDone, true);
        assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic0') > -1,
            'testTopic0 in alreadyQueriedTopics');
        assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic0') > -1,
            'testTopic0 in alreadyBlacklistedTopics');
        migratorBlacklistClient.blacklistTopic('testTopic0', function assertDoneAgain(isDone2) {
            assert.equal(isDone2, true);
        });
    });
    assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic1') === -1,
        'testTopic1 not in alreadyQueriedTopics');
    assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic1') === -1,
        'testTopic1 not in alreadyBlacklistedTopics');
    migratorBlacklistClient.blacklistTopic('testTopic1', function assertDone(isDone) {
        assert.equal(isDone, false);
        assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic1') > -1,
            'testTopic1 in alreadyQueriedTopics');
        assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic1') === -1,
            'testTopic1 not in alreadyBlacklistedTopics');
        migratorBlacklistClient.blacklistTopic('testTopic1', function assertDoneAgain(isDone2) {
            assert.equal(isDone2, false);
        });
    });
    assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic2') === -1,
        'testTopic2 not in alreadyQueriedTopics');
    assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic2') === -1,
        'testTopic2 not in alreadyBlacklistedTopics');
    migratorBlacklistClient.blacklistTopic('testTopic2', function assertDone(isDone) {
        assert.equal(isDone, true);
        assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic2') > -1,
            'testTopic2 in alreadyQueriedTopics');
        assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic2') > -1,
            'testTopic2 in alreadyBlacklistedTopics');
        migratorBlacklistClient.blacklistTopic('testTopic2', function assertDoneAgain(isDone2) {
            assert.equal(isDone2, true);
        });
    });
    assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic3') === -1,
        'testTopic3 not in alreadyQueriedTopics');
    assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic3') === -1,
        'testTopic3 not in alreadyBlacklistedTopics');
    migratorBlacklistClient.blacklistTopic('testTopic3', function assertDone(isDone) {
        assert.equal(isDone, false);
        assert.true(migratorBlacklistClient.alreadyQueriedTopics.indexOf('testTopic3') > -1,
            'testTopic3 in alreadyQueriedTopics');
        assert.true(migratorBlacklistClient.alreadyBlacklistedTopics.indexOf('testTopic3') === -1,
            'testTopic3 not in alreadyBlacklistedTopics');
        migratorBlacklistClient.blacklistTopic('testTopic3', function assertDoneAgain(isDone2) {
            assert.equal(isDone2, false);
        });
    });
    /* eslint-disable no-undef,block-scoped-var */
    setTimeout(function stopTest1() {
        assert.equal(migratorBlacklistClient.alreadyQueriedTopics.length, 4);
        assert.equal(migratorBlacklistClient.alreadyBlacklistedTopics.length, 2);
        migratorBlacklistClient.close();
        server.close();
        assert.end();
    }, 1000);
    /* eslint-enable no-undef,block-scoped-var */
});
