const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    // Implemented APIs
    // Bucket APIs
    putBucket(params, callback) {
        return this.createBucket(params, callback);
    },

    getBucket(params, callback) {
        return this.listObjects(params, callback);
    },

    // TO-DO: Implemented the following APIs
    upload(params, options, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: upload not implemented'));
    },

    // Service API
    listBuckets(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listBuckets not implemented'));
    },

    // Bucket APIs
    getBucketLocation(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketLocation not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjecVersions not implemented'));
    },

    getBucketAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketAcl not implemented'));
    },

    putBucketAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketAcl not implemented'));
    },

    putBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketWebsite not implemented'));
    },

    getBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketWebsite not implemented'));
    },

    deleteBucketWebsite(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketWebsite not implemented'));
    },

    putBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucketCors not implemented'));
    },

    getBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getBucketCors not implemented'));
    },

    deleteBucketCors(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucketCors not implemented'));
    },

    // Object APIs
    headObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headObject not implemented'));
    },

    putObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObject not implemented'));
    },

    getObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getObject not implemented'));
    },

    deleteObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObject not implemented'));
    },

    deleteObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjects not implemented'));
    },

    copyObject(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: copyObject not implemented'));
    },

    putObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectTagging not implemented'));
    },

    deleteObjectTagging(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteObjectTagging not implemented'));
    },

    putObjectAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putObjectAcl not implemented'));
    },

    getObjectAcl(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: getObjectAcl not implemented'));
    },

    // Multipart upload
    abortMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: abortMultipartUpload not implemented'));
    },

    createMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: createMultipartUpload not implemented'));
    },

    completeMultipartUpload(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: completeMultipartUpload not implemented'));
    },

    uploadPart(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPart not implemented'));
    },

    uploadPartCopy(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: uploadPartCopy not implemented'));
    },

    listParts(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription(
                'GCP: listParts not implemented'));
    },
});

Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = GCP;
