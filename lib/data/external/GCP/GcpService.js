const googleAuth = require('google-auto-auth');
const async = require('async');
const AWS = require('aws-sdk');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');

function genAuth(authParams, callback) {
    async.waterfall([
        function authKeyFile(next) {
            const authOptions = {
                scopes: authParams.scopes,
                keyFilename: authParams.keyFilename,
            };
            const auth = googleAuth(authOptions);
            auth.getAuthClient(err => {
                if (!err) return next(null, auth);
                return next(null, null);
            });
        },
        function authCredentials(authKeyfile, next) {
            const authOptions = {
                scopes: authParams.scopes,
                keyFilename: authParams.credentials,
            };
            const auth = googleAuth(authOptions);
            auth.getAuthClient(err => {
                if (!err) return next(null, authKeyfile, auth);
                return next(null, authKeyfile, null);
            });
        },
        (authKeyfile, authCredentials, next) => {
            next(null, authKeyfile || authCredentials || null);
        },
    ], (err, auth) => callback(auth));
}

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    _jsonAuth: null,
    _authParams: null,

    getToken(callback) {
        if (this._jsonAuth) return this._jsonAuth.getToken(callback);

        if (!this._authParams && this.config.authParams &&
            typeof this.config.authParams === 'object') {
            this._authParams = this.config.authParams;
        }
        return genAuth(this._authParams, auth => {
            if (auth) {
                this._jsonAuth = auth;
                return this._jsonAuth.getToken(callback);
            }
            // should never happen, but it all preconditions fails
            // can't generate tokens
            return callback(errors.InternalError.customizeDescription(
                'Unable to create a google authorizer'));
        });
    },

    getSignerClass() {
        return GcpSigner;
    },

    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

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

    deleteBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: deleteBucket not implemented'));
    },

    headBucket(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: headBucket not implemented'));
    },

    listObjects(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjects not implemented'));
    },

    listObjectVersions(params, callback) {
        return callback(errors.NotImplemented
            .customizeDescription('GCP: listObjecVersions not implemented'));
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
