'use strict';

var AWS = require('aws-sdk');
var q = require('q');
var assert = require('assert');

var ElasticTranscoder = function (options) {
    assert(options.hasOwnProperty('accessKeyId'), 'accessKeyId is required');
    assert(options.hasOwnProperty('secretAccessKey'), 'secretAccessKey is required');
    assert(options.hasOwnProperty('region'), 'region is required');

    this.transcoder = new AWS.ElasticTranscoder({
        'accessKeyId': options.accessKeyId,
        'secretAccessKey': options.secretAccessKey,
        'region': options.region
    });
};

ElasticTranscoder.prototype.createJob = function (pipelineId, optionsInput, optionsOutput) {
    var defer = q.defer();
    this.transcoder.createJob({
        PipelineId: pipelineId,
        Input: optionsInput,
        Output: optionsOutput
    }, defer.makeNodeResolver());
    return defer.promise;
};

ElasticTranscoder.prototype.listPipelines = function () {
    var defer = q.defer();
    this.transcoder.listPipelines(defer.makeNodeResolver());
    return defer.promise;
};

module.exports = ElasticTranscoder;
