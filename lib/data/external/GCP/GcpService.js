const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');
const GcpApis = require('./GcpApis');
const GcpManagedUpload = require('./GcpManagedUpload');

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    _maxConcurrent: 5,

    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    createBucketDomain(req) {
        const httpRequest = req.httpRequest;
        const bucket = req.params.Bucket;
        const endpoint = httpRequest.endpoint;
        if (bucket && httpRequest && httpRequest.path) {
            httpRequest.path = httpRequest.path.replace(`/${bucket}`, '');
            if (httpRequest.path[0] !== '/') {
                httpRequest.path = `/${httpRequest.path}`;
            }
            const port = endpoint.port;
            endpoint.hostname = `${bucket}.${endpoint.hostname}`;
            if (port !== 80 && port !== 443) {
                endpoint.host = `${endpoint.hostname}:${endpoint.port}`;
            } else {
                endpoint.host = endpoint.hostname;
            }
        }
    },

    populateURI(req) {
        if (req) {
            const httpRequest = req.httpRequest;
            const bucket = req.params && req.params.Bucket;
            if (bucket && httpRequest && httpRequest.path &&
                !this.config.s3ForcePathStyle) {
                this.createBucketDomain(req);
                httpRequest.virtualHostedBucket = bucket;
            }
        }
    },

    setupRequestListeners(req) {
        req.addListener('build', this.populateURI.bind(this));
    },

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
