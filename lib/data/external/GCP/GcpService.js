const googleAuth = require('google-auto-auth');
const async = require('async');
const AWS = require('aws-sdk');
const stream = require('stream');
const { errors } = require('arsenal');
const Service = AWS.Service;

const GcpSigner = require('./GcpSigner');
const GcpApis = require('./GcpApis');

/**
 * genAuth - create a google authorizer for generating request tokens
 * @param {object} authParams - params that contains the credentials for
 * generating the authorizer
 * @param {function} callback - callback function to call with the authorizer
 * @return {undefined}
 */
function genAuth(authParams, callback) {
    async.tryEach([
        function authKeyFile(next) {
            const authOptions = {
                scopes: authParams.scopes,
                keyFilename: authParams.keyFilename,
            };
            const auth = googleAuth(authOptions);
            auth.getToken(err => next(err, auth));
        },
        function authCredentials(next) {
            const authOptions = {
                scopes: authParams.scopes,
                credentials: authParams.credentials,
            };
            const auth = googleAuth(authOptions);
            auth.getToken(err => next(err, auth));
        },
    ], (err, result) => callback(err, result));
}

AWS.apiLoader.services.gcp = {};
const GCP = Service.defineService('gcp', ['2017-11-01'], {
    _maxConcurrent: 5,
    _maxRetries: 5,
    _jsonAuth: null,
    _authParams: null,

    /**
     * getToken - generate a token for authorizing JSON API requests
     * @param {function} callback - callback function to call with the
     * generated token
     * @return {undefined}
     */
    getToken(callback) {
        if (this._jsonAuth) {
            return this._jsonAuth.getToken(callback);
        }

        if (!this._authParams && this.config.authParams &&
            typeof this.config.authParams === 'object') {
            this._authParams = this.config.authParams;
        }
        return genAuth(this._authParams, (err, auth) => {
            if (!err) {
                this._jsonAuth = auth;
                return this._jsonAuth.getToken(callback);
            }
            // should never happen, but if all preconditions fails
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

    // Implemented APIs
    // Bucket API
    getBucket(params, callback) {
        return this.listObjects(params, callback);
    },

    // Object APIs
    upload(params, callback) {
        if (params.Body instanceof stream) {
            return callback(errors.NotImplemented
                .customizeDescription(
                    'GCP: Upload with stream body not implemented'));
        }
        return this.putObject(params, callback);
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
