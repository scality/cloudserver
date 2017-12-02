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

    getSignerClass: function getSignerClass() {
        return GcpSigner;
    },

    validateService: function validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    upload: function upload(params, options, callback) {
        /* eslint-disable no-param-reassign */
        if (typeof options === 'function' && callback === undefined) {
            callback = options;
            options = null;
        }
        /* eslint-disable no-param-reassign */

        return callback(errors.NotImplemented
            .customizeDescription('GCP: upload not implemented'));
    },

    putObject: function putObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObject not implemented'));
    },

    getObject: function getObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getObject not implemented'));
    },

    headObject: function headObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headObject not implemented'));
    },

    deleteObject: function deleteObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObject not implemented'));
    },

    copyObject: function copyObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: copyObject not implemented'));
    },

    putObjectTagging: function putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implementend'));
    },

    deleteObjectTagging: function deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implementend'));
    },

    abortMultipartUpload: function abortMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: abortMultipartUpload not implementend'));
    },

    createMultipartUpload: function createMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: createMultipartUpload not implementend'));
    },

    completeMultipartUpload:
    function completeMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: completeMultipartUpload not implementend'));
    },

    uploadPart: function uploadPart(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPart not implementend'));
    },

    uploadPartCopy: function uploadPartCopy(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPartCopy not implementend'));
    },

    listParts: function listParts(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: listParts not implementend'));
    },
});

module.exports = GCP;
