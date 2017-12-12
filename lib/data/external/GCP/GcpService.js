const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01']);
Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

Object.assign(GCP.prototype, {

    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    upload(params, options, callback) {
        /* eslint-disable no-param-reassign */
        if (typeof options === 'function' && callback === undefined) {
            callback = options;
            options = null;
        }
        options = options || {};
        options = AWS.util.merge(options, { service: this, params });
        /* eslint-disable no-param-reassign */

        const uploader = new AWS.S3.ManagedUpload(options);
        if (typeof callback === 'function') uploader.send(callback);
        return uploader;
    },

    copyObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: copyObject not implemented'));
    },

    putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implementend'));
    },

    deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implementend'));
    },

    abortMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: abortMultipartUpload not implementend'));
    },

    createMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: createMultipartUpload not implementend'));
    },

    completeMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: completeMultipartUpload not implementend'));
    },

    uploadPart(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPart not implementend'));
    },

    uploadPartCopy(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPartCopy not implementend'));
    },

    listParts(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: listParts not implementend'));
    },
});

module.exports = GCP;
