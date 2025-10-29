const { v4: uuidv4 } = require('uuid');

class DummyService {
    constructor(config = {}) {
        this.versioning = config.versioning;
    }

    // SDK v3 command-based interface
    async send(command) {
        const commandName = command.constructor.name;
        const input = command.input;

        switch (commandName) {
            case 'HeadBucketCommand':
                return {};
            case 'GetBucketVersioningCommand':
                if (this.versioning) {
                    return { Status: 'Enabled' };
                }
                return {};
            case 'HeadObjectCommand':
                return {
                    ContentLength: 1024 * 1024 * 1024,
                };
            case 'CompleteMultipartUploadCommand': {
                const retObj = {
                    Bucket: input.Bucket,
                    Key: input.Key,
                    ETag: `"${uuidv4().replace(/-/g, '')}"`,
                    ContentLength: 1024 * 1024 * 1024,
                };
                if (this.versioning) {
                    retObj.VersionId = uuidv4().replace(/-/g, '');
                }
                return retObj;
            }
            case 'PutObjectCommand': {
                const retObj = {
                    ETag: `"${uuidv4().replace(/-/g, '')}"`,
                };
                if (this.versioning) {
                    retObj.VersionId = uuidv4().replace(/-/g, '');
                }
                return retObj;
            }
            case 'CopyObjectCommand': {
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
                return retObj;
            }
            default:
                throw new Error(`Unsupported command: ${commandName}`);
        }
    }

    // Legacy SDK v2 callback-based interface (for backwards compatibility)
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
