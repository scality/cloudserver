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
    getObject(params, callback) {
        const retObj = {};
        if (this.versioning) {
            retObj.VersionId = uuidv4().replace(/-/g, '');
            retObj.Status = 'Enabled';
        }
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
    deleteObject(params, callback) {
        return callback(null, {});
    }
    uploadPart(params, callback) {
        const retObj = {
            ETag: `"${uuidv4().replace(/-/g, '')}"`,
            PartNumber: params.PartNumber,
            Size: params.ContentLength || 0,
            LastModified: new Date().toISOString(),
        };
        return callback(null, retObj);
    }
    listParts(params, callback) {
        const parts = [1, 2, 3].map(n => ({
            ETag: `"${uuidv4().replace(/-/g, '')}"`,
            PartNumber: n,
            Size: 1,
            LastModified: new Date().toISOString(),
        }));
        return callback(null, { Parts: parts, IsTruncated: false });
    }

    // AWS SDK v3 compatibility shim (supports promise and callback styles)
    send(command, cb) {
        const name = command && command.constructor && command.constructor.name;
        const runner = handler => {
            switch (name) {
                case 'HeadBucketCommand':
                    return this.headBucket(command.input, handler);
                case 'GetBucketVersioningCommand':
                    return this.getBucketVersioning(command.input, handler);
                case 'HeadObjectCommand':
                    return this.headObject(command.input, handler);
                case 'GetObjectCommand':
                    return this.getObject(command.input, handler);
                case 'CompleteMultipartUploadCommand':
                    return this.completeMultipartUpload(command.input, handler);
                case 'PutObjectCommand':
                    return this.putObject(command.input, handler);
                case 'CopyObjectCommand':
                    return this.copyObject(command.input, handler);
                case 'DeleteObjectCommand':
                    return this.deleteObject(command.input, handler);
                case 'UploadPartCommand':
                    return this.uploadPart(command.input, handler);
                case 'ListPartsCommand':
                    return this.listParts(command.input, handler);
                default:
                    return handler(null, {});
            }
        };

        if (typeof cb === 'function') {
            return runner(cb);
        }

        return new Promise((resolve, reject) => {
            runner((err, data) => (err ? reject(err) : resolve(data)));
        });
    }
}

module.exports = DummyService;
