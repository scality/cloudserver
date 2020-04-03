const uuid = require('uuid/v4');

class DummyService {
    constructor(config = {}) {
        this.versioning = config.versioning;
    }
    headBucket(params, callback) {
        return callback();
    }
    getBucketVersioning(params, callback) {
        if (this.versioning) {
            return callback(null, { Status: 'Enabled' });
        }
        return callback(null, {});
    }
    headObject(params, callback) {
        const retObj = {
            ContentLength: `${1024 * 1024 * 1024}`,
        };
        return callback(null, retObj);
    }
    completeMultipartUpload(params, callback) {
        const retObj = {
            Bucket: params.Bucket,
            Key: params.Key,
            ETag: `"${uuid().replace(/-/g, '')}"`,
            ContentLength: `${1024 * 1024 * 1024}`,
        };
        if (this.versioning) {
            retObj.VersionId = uuid().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    upload(params, callback) {
        this.putObject(params, callback);
    }
    putObject(params, callback) {
        const retObj = {
            ETag: `"${uuid().replace(/-/g, '')}"`,
        };
        if (this.versioning) {
            retObj.VersionId = uuid().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    copyObject(params, callback) {
        const retObj = {
            CopyObjectResult: {
                ETag: `"${uuid().replace(/-/g, '')}"`,
                LastModified: new Date().toISOString(),
            },
            VersionId: null,
        };
        if (this.versioning) {
            retObj.VersionId = uuid().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    // To-Do: add tests for other methods
}

module.exports = DummyService;
