const { v4: uuidv4 } = require('uuid');

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
            ETag: `"${uuidv4().replace(/-/g, '')}"`,
            ContentLength: `${1024 * 1024 * 1024}`,
        };
        if (this.versioning) {
            retObj.VersionId = uuidv4().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    upload(params, callback) {
        this.putObject(params, callback);
    }
    putObject(params, callback) {
        const retObj = {
            ETag: `"${uuidv4().replace(/-/g, '')}"`,
        };
        if (this.versioning) {
            retObj.VersionId = uuidv4().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    copyObject(params, callback) {
        const retObj = {
            CopyObjectResult: {
                ETag: `"${uuidv4().replace(/-/g, '')}"`,
                LastModified: new Date().toISOString(),
            },
            VersionId: null,
        };
        if (this.versioning) {
            retObj.VersionId = uuidv4().replace(/-/g, '');
        }
        return callback(null, retObj);
    }
    // To-Do: add tests for other methods
}

module.exports = DummyService;
