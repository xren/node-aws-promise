'use strict';

var q = require('q');
var AWS = require('aws-sdk');
var SQS = require('../lib/sqs');
var _ = require('lodash');
var assert = require('assert');

var guid = function () {
    return 'xxxxxxxx-xxxx-xxxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function () {
        return Math.floor(Math.random() * 16).toString(16);
    });
};

var deferredLoop = function (func, times) {
    if(times === 0) {
        return q.resolve();
    } else {
        var next = _.partial(deferredLoop, func, --times);
        return func().fin(next);
    }
};

describe('sqs test suite', function () {
    this.timeout(6000);
    beforeEach(function createQueues(done) {
        this.queue = new SQS({
            accessKeyId: process.env.AWS_ACCESS_ID,
            secretAccessKey: process.env.AWS_ACCESS_SECRET,
            region: process.env.AWS_REGION
        });

        var queueName = 'content_test_queue_' + guid();

        var defer = q.defer();

        q.resolve().then(_.bind(function () {
            return this.queue.createQueue(queueName);
        }, this)).then(_.bind(function (queueUrl) {
            this.queueUrl = queueUrl;
            done();
        }, this)).fail(function (err) {
            done(new Error(err));
        });
        this.queue.createQueue(queueName);
    });

    afterEach(function deleteQueues(done) {
        q.resolve().then(_.bind(function () {
            return this.queue.deleteQueue(this.queueUrl);
        }, this)).then(function () {
            done();
        }).fail(function (err) {
            done(new Error(err));
        });
    });

    it('pushes then pops a message', function (done) {
        var sentMessage = guid();
        q.resolve().then(_.bind(function () {
            return this.queue.push(sentMessage);
        }, this)).then(_.bind(function () {
            this.queue.pop(q.resolve()).then(function (receivedMessage) {
                if (receivedMessage === sentMessage) {
                    done();
                } else {
                    done(new Error('received message did not match the message sent'));
                }
            }, function (err) {
                done(new Error(err));
            });
        }, this)).fail(function (err) {
            done(new Error(err));
        });
    });

    it('pushes then pops json', function (done) {
        var sentMessage = {
            a: guid(),
            b: guid()
        };
        this.queue.push(sentMessage).then(_.bind(function () {
            this.queue.pop(q.resolve()).then(function (receivedMessage) {
                if (_.isEqual(receivedMessage, sentMessage)) {
                    done();
                } else {
                    done(new Error('received message did not match the message sent'));
                }
            }, function (err) {
                done(new Error(err));
            });
        }, this), function (err) {
            done(new Error(err));
        });
    });

    it('pushes then pops the message twice after a failed task argument', function (done) {
        this.timeout(30000);
        var sentMessage = guid();
        this.queue.push(sentMessage);

        this.queue.pop(q.reject()).then(_.bind(function (receivedMessage) {
            assert(receivedMessage === sentMessage, 'received an unexpected message');
            return this.queue.pop(q.resolve()).then(function (receivedMessage) {
                assert(receivedMessage === sentMessage, 'received an unexpected message');
                done();
           });
        }, this)).fail(function (err) {
            done(new Error(err));
        });
    });

    it('makes sure that message visibility is set, rather than changed...aws documentation is bullshit', function (done) {
        this.timeout(10000);
        this.queue.push(guid()).then(_.bind(function () {
            this.queue._receiveMessage().then(_.bind(function (message) {
                var start = new Date().getTime();
                var SET_VISIBILITY = 3;
                q.all([
                    this.queue._changeMessageVisibility(message, SET_VISIBILITY),
                    this.queue._changeMessageVisibility(message, SET_VISIBILITY),
                    this.queue._changeMessageVisibility(message, SET_VISIBILITY),
                    this.queue._changeMessageVisibility(message, SET_VISIBILITY)
                ]).then(_.bind(function () {
                    this.queue.pop(q.resolve()).then(function (message) {
                        var now = new Date().getTime();
                        var delta = (now - start) / 1000.0;
                        if (delta > 2 * SET_VISIBILITY) {
                            done(new Error('message expiration happend to slowly'));
                        } else {
                            done();
                        }
                    });
                }, this), done);
            }, this), done);
       }, this), done);
    });

    it('pushes, pops and does not successfully pop again even after the initial visibility timeout has expired', function (done) {
        this.timeout(80000);
        this.queue.push(guid());

        var count = 0;
        var receiveMessage = _.after(2, function (receivedMessage) {
            done(new Error('the message became visible again!'));
        });
        this.queue.pop(q.defer().promise).then(receiveMessage);
        this.queue.pop(q.defer().promise).then(receiveMessage);
        setTimeout(function () {
            done();
        }, 75000);
    });

    it('pops then waits for a push', function (done) {
        this.timeout(10000);
        var sentMessage = guid();
        this.queue.pop(q.resolve()).then(function (receivedMessage) {
            if (receivedMessage === sentMessage) {
                done();
            } else {
                done(new Error('received message did not match the message sent'));
            }
        });
        setTimeout(_.bind(function () {
            this.queue.push(sentMessage);
        }, this), 8000);
    });

    it('pushes 10 messages then pops them', function (done) {
        this.timeout(25000);

        var messages = {};

        var pushRequests = _.bind(function () {
            return deferredLoop(_.bind(function () {
                var message = guid();
                messages[message] = false;
                return this.queue.push(message);
            }, this), 10);
        }, this);

        var popRequests = _.bind(function () {
            return deferredLoop(_.bind(function () {
                return this.queue.pop(q.resolve()).then(function (message) {
                    delete messages[message];
                    return q.resolve();
                });
            }, this), 10);
        }, this);

        pushRequests()
            .then(popRequests)
            .then(function () {
                assert(_.keys(messages).length === 0, 'expect all messages to be popped');
                done();
            }, function (err) {
                done(new Error(err));
            });
    });

    it('pushes then pops 10 times', function (done) {
        this.timeout(25000);
        deferredLoop(_.bind(function () {
            var message = guid();
            return this.queue.push(message)
                .then(this.queue.pop.bind(this.queue, q.resolve()))
                .then(function (result) {
                    assert(message === result, 'wrong message received');
                    return q.resolve();
                });
        }, this), 10).then(function () {
            done();
        }, function (err) {
            done(new Error(err));
        });
    });

    it('pushes messages with spaces', function (done) {
        var sentMessage = 'asdf asdf asdf';
        this.queue.push(sentMessage).then(_.bind(function () {
            this.queue.pop(q.resolve()).then(function (receivedMessage) {
                if (_.isEqual(receivedMessage, sentMessage)) {
                    done();
                } else {
                    done(new Error('received message did not match the message sent'));
                }
            }, function (err) {
                done(new Error(err));
            });
        }, this), function (err) {
            done(new Error(err));
        });
    });

    it('pushes messages with parenthesis', function (done) {
        var sentMessage = '(';
        this.queue.push(sentMessage).then(_.bind(function () {
            this.queue.pop(q.resolve()).then(function (receivedMessage) {
                if (_.isEqual(receivedMessage, sentMessage)) {
                    done();
                } else {
                    done(new Error('received message did not match the message sent'));
                }
            }, function (err) {
                done(new Error(err));
            });
        }, this), function (err) {
            done(new Error(err));
        });
    });

    it('pushes json messages with parenthesis', function (done) {
        var sentMessage = {
            '(': '&1234'
        };
        this.queue.push(sentMessage).then(_.bind(function () {
            this.queue.pop(q.resolve()).then(function (receivedMessage) {
                if (_.isEqual(receivedMessage, sentMessage)) {
                    done();
                } else {
                    done(new Error('received message did not match the message sent'));
                }
            }, function (err) {
                done(new Error(err));
            });
        }, this), function (err) {
            done(new Error(err));
        });
    });
});


