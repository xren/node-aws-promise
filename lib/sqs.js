'use strict';

var AWS = require('aws-sdk');
var assert = require('assert');
var q = require('q');
var crypto = require('crypto');
var _ = require('lodash');
var url = require('url');
var request = require('request');

// http://docs.aws.amazon.com/general/latest/gr/signature-version-2.html
var sign = function (type, queryUrl, parameters, secret) {
    var raw = type + '\n';
    raw += url.parse(queryUrl).host + '\n';
    raw += url.parse(queryUrl).path + '\n';

    var sorted = _.map(parameters, function (v, k) {
        return [k, v];
    }).sort(function (item1, item2) {
        return item1[0].localeCompare(item2[0]);
    });

    raw += _.reduce(sorted, function (memo, pair) {
        return memo + (memo.length ? '&' : '') + encodeURIComponent(pair[0]) + '=' + encodeURIComponent(pair[1]);
    }, '');
    var hmac = crypto.createHmac('sha256', secret);
    hmac.update(raw);
    return hmac.digest('base64');
};

var SQS = function (options) {
    assert(options.hasOwnProperty('accessKeyId'), 'accessKeyId is required');
    assert(options.hasOwnProperty('secretAccessKey'), 'secretAccessKey is required');
    assert(options.hasOwnProperty('region'), 'region is required');

    this.options = options;
    this.accessKeyId = options.accessKeyId;
    this.secretAccessKey = options.secretAccessKey;
    this.region = options.region;
    if (options.hasOwnProperty('queueUrl')) {
        this.queueUrl = options.queueUrl;
    }

    this.sqs = new AWS.SQS({
        'accessKeyId': this.accessKeyId,
        'secretAccessKey': this.secretAccessKey,
        'region': this.region
    });
};

SQS.prototype.createQueue = function (queueName, attributes) {
    attributes = attributes || {};
    assert(_.isPlainObject(attributes), 'attributes should be an plain object');
    var defer = q.defer();
    this.sqs.createQueue({
        QueueName: queueName,
        Attributes: attributes || {}
    }, defer.makeNodeResolver());
    return defer.promise.then(_.bind(function (data) {
        this.queueUrl = data.QueueUrl;
        return q.resolve(data.QueueUrl);
    }, this));
};

SQS.prototype.deleteQueue = function (queueUrl) {
    var defer = q.defer();
    this.sqs.deleteQueue({
        QueueUrl: queueUrl
    }, defer.makeNodeResolver());
    return defer.promise;
};

SQS.prototype._deleteMessage = function (message) {
    assert(this.queueUrl, 'queueUrl is required');

    var deleteRequest = q.defer();
    var parameters = {
        Action: 'DeleteMessage',
        AWSAccessKeyId: this.accessKeyId,
        Version: '2012-11-05',
        Timestamp: new Date().toISOString(),
        SignatureVersion: 2,
        SignatureMethod: 'HmacSHA256',
        ReceiptHandle: message.ReceiptHandle
    };
    parameters.Signature = sign('GET', this.queueUrl, parameters, this.secretAccessKey);
    request.get({
        url: this.queueUrl,
        qs: parameters,
        json: true
    }, deleteRequest.makeNodeResolver());
    return deleteRequest.promise;
};

SQS.prototype._changeMessageVisibility = function (message, timeout) {
    assert(this.queueUrl, 'queueUrl is required');

    var changeVisibilityRequest = q.defer();
    var parameters = {
        Action: 'ChangeMessageVisibility',
        AWSAccessKeyId: this.accessKeyId,
        Version: '2012-11-05',
        Timestamp: new Date().toISOString(),
        SignatureVersion: 2,
        SignatureMethod: 'HmacSHA256',
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
    };
    parameters.Signature = sign('GET', this.queueUrl, parameters, this.secretAccessKey);
    request.get({
        url: this.queueUrl,
        qs: parameters,
        json: true
    }, changeVisibilityRequest.makeNodeResolver());
    return changeVisibilityRequest.promise;
};

var parseReceiveMessageResults = function (res, receiveData) {
    if (res.statusCode !== 200) {
        return q.reject(receiveData.Error.Message);
    }

    if (receiveData &&
        receiveData.hasOwnProperty('ReceiveMessageResponse') &&
        receiveData.ReceiveMessageResponse.hasOwnProperty('ReceiveMessageResult') &&
        receiveData.ReceiveMessageResponse.ReceiveMessageResult.hasOwnProperty('messages') &&
        receiveData.ReceiveMessageResponse.ReceiveMessageResult.messages.length > 0)
    {
        assert(receiveData.ReceiveMessageResponse.ReceiveMessageResult.messages.length === 1, 'only one message at a time');
        var message = receiveData.ReceiveMessageResponse.ReceiveMessageResult.messages[0];
        return q.resolve(message);
    } else {
        return q.reject();
    }
};

SQS.prototype._receiveMessage = function () {
    assert(this.queueUrl, 'queueUrl is required');

    var receiveRequest = q.defer();
    var parameters = {
        Action: 'ReceiveMessage',
        AWSAccessKeyId: this.accessKeyId,
        Version: '2012-11-05',
        Timestamp: new Date().toISOString(),
        SignatureVersion: 2,
        SignatureMethod: 'HmacSHA256',
        MaxNumberOfMessages: 1,
        VisibilityTimeout: 30,
        WaitTimeSeconds: 20
    };
    parameters.Signature = sign('GET', this.queueUrl, parameters, this.secretAccessKey);
    request.get({
        url: this.queueUrl,
        qs: parameters,
        json: true
    }, receiveRequest.makeNodeResolver());
    return receiveRequest.promise.spread(parseReceiveMessageResults);
};

// http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Query_QueryReceiveMessage.html
SQS.prototype.pop = function (task) {
    assert(this.queueUrl, 'queueUrl is required');
    assert(typeof task !== 'undefined', 'pop needs a task promise to know when it is safe to delete the message');

    var retry = this.pop.bind(this, task);
    return this._receiveMessage().then(function (message) {

        var extendMessageTimeoutInterval = setInterval(function () {
            this._changeMessageVisibility(message, 30);
        }.bind(this), 15000);

        task.then(function () {
            clearInterval(extendMessageTimeoutInterval);
            // if we were successful, delete the message
            this._deleteMessage(message);
        }.bind(this), function () {
            clearInterval(extendMessageTimeoutInterval);
            // if we've failed for some reason, make the message immediately visible
            this._changeMessageVisibility(message, 0);
        }.bind(this));

        return q.resolve(JSON.parse(unescape(decodeURIComponent(message.Body))));
    }.bind(this)).fail(function () {
        return retry();
    });
};

SQS.prototype.push = function (message) {
    assert(this.queueUrl, 'queueUrl is required');
    var pushRequest = q.defer();
    var parameters = {
        Action: 'SendMessage',
        AWSAccessKeyId: this.accessKeyId,
        Version: '2012-11-05',
        Timestamp: new Date().toISOString(),
        SignatureVersion: 2,
        SignatureMethod: 'HmacSHA256',
        MessageBody: escape(encodeURIComponent(JSON.stringify(message))),
        DelaySeconds: 0
    };
    parameters.Signature = sign('GET', this.queueUrl, parameters, this.secretAccessKey);
    request.get({
        url: this.queueUrl,
        qs: parameters,
        json: true
    }, pushRequest.makeNodeResolver());
    return pushRequest.promise.spread(function (res, body) {
        if (res.statusCode !== 200) {
            return q.reject(body.Error.Message);
        } else {
            return q.resolve(body);
        }
    });
};

module.exports = SQS;