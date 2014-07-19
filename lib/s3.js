'use strict';

var AWS = require('aws-sdk');
var q = require('q');
var assert = require('assert');

var S3 = function (options) {
    assert(options.hasOwnProperty('accessKeyId'), 'accessKeyId is required');
    assert(options.hasOwnProperty('secretAccessKey'), 'secretAccessKey is required');
    assert(options.hasOwnProperty('region'), 'region is required');

    this.s3 = new AWS.S3({
        'accessKeyId': options.accessKeyId,
        'secretAccessKey': options.secretAccessKey,
        'region': options.region
    });
};

S3.prototype.putObject = function (params) {
    var defer = q.defer();
    this.s3.putObject(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.getObject = function (params) {
    var defer = q.defer();
    this.s3.getObject(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.listBuckets = function () {
    var defer = q.defer();
    this.s3.listBuckets({}, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.listObjects = function (params) {
    var defer = q.defer();
    this.s3.listObjects(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.copyObject = function (params) {
    // CAUTION: the required argument 'CopySource' should
    // be the full path include bucket name to the source file
    var defer = q.defer();
    this.s3.copyObject(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.deleteObject = function (params) {
    var defer = q.defer();
    this.s3.deleteObject(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.putObjectAcl = function (params) {
    var defer = q.defer();
    this.s3.putObjectAcl(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.headObject = function (params) {
    var defer = q.defer();
    this.s3.headObject(params, defer.makeNodeResolver());
    return defer.promise;
};

S3.prototype.putObject = function (params) {
    var defer = q.defer();
    this.s3.putObject(params, defer.makeNodeResolver());
    return defer.promise;
};

module.exports = S3;

