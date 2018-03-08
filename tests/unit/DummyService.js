const uuid = require('uuid/v4');

class DummyService {
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
            VersionId: uuid().replace(/-/g, ''),
            ETag: `"${uuid().replace(/-/g, '')}"`,
            ContentLength: `${1024 * 1024 * 1024}`,
        };
        return callback(null, retObj);
    }
    deleteObject(params, callback) {
        return callback();
    }
    // To-Do: add tests for other methods
}

module.exports = DummyService;
