const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpApis = require('./GcpApis');
const GcpServiceSetup = require('./GcpServiceSetup');
const GcpManagedUpload = require('./GcpManagedUpload');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01']);

Object.assign(GCP.prototype, GcpServiceSetup, {
    _maxConcurrent: 5,

    // Implemented APIs
    // Bucket API
    getBucket(params, callback) {
        return this.listObjects(params, callback);
    },

    // Object APIs
    upload(params, callback) {
        try {
            const uploader = new GcpManagedUpload(this, params);
            return uploader.send(callback);
        } catch (err) {
            return callback(err);
        }
    },

    putObjectCopy(params, callback) {
        return this.copyObject(params, callback);
    },

    // TO-DO: Implement the following APIs
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

    deleteBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucket not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjectVersions not implemented'));
    },

    createBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: createBucket not implemented'));
    },

    putBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: putBucket not implemented'));
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
});

Object.assign(GCP.prototype, GcpApis);

Object.defineProperty(AWS.apiLoader.services.gcp, '2017-11-01', {
    get: function get() {
        const model = require('./gcp-2017-11-01.api.json');
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = GCP;
