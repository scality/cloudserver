const async = require('async');
const assert = require('assert');
const { createHash } = require('crypto');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
    CopyObjectCommand,
    PutBucketVersioningCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    ListObjectVersionsCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const MockUtapi = require('../utilities/mock/Utapi');
const getConfig = require('../functional/aws-node-sdk/test/support/config');
const WAIT_MS = 100;
let s3Client = null;

/**
 * Creates an S3 client that uses MD5 checksums for DeleteObjects operations
 * Based on https://github.com/aws/aws-sdk-js-v3/blob/main/supplemental-docs/MD5_FALLBACK.md
 */
function createS3ClientWithMD5(configuration = {}) {
    const client = new S3Client(configuration);

    client.middlewareStack.add(
        (next, context) => async args => {
            const needsMD5 = context.commandName === 'DeleteObjectsCommand' ||
                             context.commandName === 'UploadPartCommand' ||
                             context.commandName === 'CompleteMultipartUploadCommand';
            
            if (!needsMD5) {
                return next(args);
            }

            const headers = args.request.headers;

            // Remove any checksum headers
            Object.keys(headers).forEach(header => {
                if (
                    header.toLowerCase().startsWith('x-amz-checksum-') ||
                    header.toLowerCase().startsWith('x-amz-sdk-checksum-')
                ) {
                    delete headers[header];
                }
            });

            // Add MD5
            if (args.request.body) {
                const bodyContent = Buffer.from(args.request.body);
                // Create a new hash instance for each request
                headers['Content-MD5'] = createHash('md5').update(bodyContent).digest('base64');
            }

            return await next(args);
        },
        {
            step: 'build',
        }
    );
    return client;
}

function wait(timeoutMs, cb) {
    setTimeout(cb, timeoutMs);
}

function createBucket(bucket, cb) {
    return s3Client.send(new CreateBucketCommand({ Bucket: bucket }))
        .then(data => cb(null, data))
        .catch(cb);
}

function deleteBucket(bucket, cb) {
    return s3Client.send(new DeleteBucketCommand({ Bucket: bucket }))
        .then(() => cb())
        .catch(cb);
}

function putObject(bucket, key, size, cb) {
    const body = Buffer.alloc(size);
    const params = {
        Bucket: bucket,
        Key: key,
        Body: body,
    };
    return s3Client.send(new PutObjectCommand(params))
        .then(data => cb(null, data))
        .catch(cb);
}

function deleteObject(bucket, key, cb) {
    return s3Client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }))
        .then(() => cb())
        .catch(cb);
}

function deleteObjects(bucket, keys, cb) {
    const objects = keys.map(key => ({ Key: key }));
    const deleteRequest = { Objects: objects, Quiet: true };
    const params = {
        Bucket: bucket,
        Delete: deleteRequest,
    };
    return s3Client.send(new DeleteObjectsCommand(params))
        .then(() => cb())
        .catch(cb);
}

function copyObject(bucket, key, cb) {
    const params = { Bucket: bucket, CopySource: `${bucket}/${key}`, Key: `${key}-copy` };
    return s3Client.send(new CopyObjectCommand(params))
        .then(() => cb())
        .catch(cb);
}

function enableVersioning(bucket, enable, cb) {
    const versioningStatus = { Status: enable ? 'Enabled' : 'Disabled' };
    const params = { Bucket: bucket, VersioningConfiguration: versioningStatus };
    return s3Client.send(new PutBucketVersioningCommand(params))
        .then(() => cb())
        .catch(cb);
}

function deleteVersionList(versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const deleteRequest = { Objects: [] };
    versionList.forEach(version => {
        deleteRequest.Objects.push({ Key: version.Key, VersionId: version.VersionId });
    });
    const params = {
        Bucket: bucket,
        Delete: deleteRequest,
    };
    return s3Client.send(new DeleteObjectsCommand(params))
        .then(() => callback())
        .catch(callback);
}

function removeAllVersions(params, callback) {
    const bucket = params.Bucket;
    async.waterfall([
        cb => s3Client.send(new ListObjectVersionsCommand(params))
            .then(data => cb(null, data))
            .catch(cb),
        (data, cb) => deleteVersionList(data.DeleteMarkers, bucket, err => cb(err, data)),
        (data, cb) => deleteVersionList(data.Versions, bucket, err => cb(err, data)),
        (data, cb) => {
            if (data.IsTruncated) {
                const params = { Bucket: bucket, KeyMarker: data.NextKeyMarker, 
                VersionIdMarker: data.NextVersionIdMarker };
                return removeAllVersions(params, cb);
            }
            return cb();
        },
    ], callback);
}

function objectMPU(bucket, key, parts, partSize, callback) {
    let ETags = [];
    let uploadId = null;
    const partNumbers = Array.from(Array(parts).keys());
    const initiateMPUParams = { Bucket: bucket, Key: key };
    return async.waterfall([
        next => s3Client.send(new CreateMultipartUploadCommand(initiateMPUParams))
            .then(data => {
                uploadId = data.UploadId;
                return next();
            })
            .catch(next),
        next =>
            async.mapLimit(partNumbers, 1, (partNumber, callback) => {
                const body = Buffer.alloc(partSize);
                const uploadPartParams = {
                    Bucket: bucket,
                    Key: key,
                    PartNumber: partNumber + 1,
                    UploadId: uploadId,
                    Body: body,
                };
                return s3Client.send(new UploadPartCommand(uploadPartParams))
                    .then(data => callback(null, data.ETag))
                    .catch(callback);
            }, (err, results) => {
                if (err) {
                    return next(err);
                }
                ETags = results;
                return next();
            }),
        next => {
            const completeRequest = { Parts: partNumbers.map(n => ({ ETag: ETags[n], PartNumber: n + 1 })) };
            const params = {
                Bucket: bucket,
                Key: key,
                MultipartUpload: completeRequest,
                UploadId: uploadId,
            };
            return s3Client.send(new CompleteMultipartUploadCommand(params))
                .then(data => next(null, data))
                .catch(next);
        },
    ], callback);
}

function removeVersions(buckets, cb) {
    return async.each(buckets, (bucket, done) => removeAllVersions({ Bucket: bucket }, done), cb);
}

function getObject(bucket, key, cb) {
    return s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
        .then(data => cb(null, data))
        .catch(cb);
}

describe('utapi v2 metrics incoming and outgoing bytes', function t() {
    this.timeout(30000);
    const utapi = new MockUtapi();

    function checkMetrics(inBytes, outBytes, objCount) {
        const accountMetrics = utapi.getAccountMetrics();
        assert(accountMetrics);
        assert.strictEqual(accountMetrics.incomingBytes, inBytes);
        assert.strictEqual(accountMetrics.outgoingBytes, outBytes);
        assert.strictEqual(accountMetrics.numberOfObjects, objCount);
    }

    before(() => {
        s3Client = createS3ClientWithMD5(getConfig('default'));
        utapi.start();
    });
    afterEach(() => {
        utapi.reset();
    });
    after(() => {
        utapi.stop();
    });
    it('should set metrics for createBucket and deleteBucket', done => {
        const bucket = 'bucket1';
        async.series([
            next => createBucket(bucket, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
        ], done);
    });
    it('should set metrics for putObject and deleteObject', done => {
        const bucket = 'bucket2';
        const objectSize = 1024 * 1024;
        const obj1Size = objectSize * 1;
        const obj2Size = objectSize * 2;
        const key1 = '1.txt';
        const key2 = '2.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key1, obj1Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size, 0, 1);
                next();
            }),
            next => putObject(bucket, key2, obj2Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 2);
                next();
            }),
            next => deleteObject(bucket, key1, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj2Size, 0, 1);
                next();
            }),
            next => deleteObject(bucket, key2, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for copyObject', done => {
        const bucket = 'bucket3';
        const objectSize = 1024 * 1024 * 2;
        const key = '3.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key, objectSize, next),
            next => copyObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize * 2, 0, 2);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize, 0, 1);
                next();
            }),
            next => deleteObject(bucket, `${key}-copy`, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for getObject', done => {
        const bucket = 'bucket4';
        const objectSize = 1024 * 1024 * 2;
        const key = '4.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key, objectSize, next),
            next => getObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize, objectSize, 1);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, objectSize, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiObjectDelete', done => {
        const bucket = 'bucket5';
        const objectSize = 1024 * 1024;
        const obj1Size = objectSize * 2;
        const obj2Size = objectSize * 1;
        const key1 = '1.txt';
        const key2 = '2.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key1, obj1Size, next),
            next => wait(WAIT_MS, next),
            next => putObject(bucket, key2, obj2Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 2);
                next();
            }),
            next => deleteObjects(bucket, [key1, key2], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiPartUpload', done => {
        const bucket = 'bucket6';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '6.txt';
        async.series([
            next => createBucket(bucket, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts, 0, 1);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics in versioned bucket', done => {
        const bucket = 'bucket7';
        const objectSize = 1024 * 1024;
        const key = '7.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => putObject(bucket, key, objectSize, next),
            next => wait(WAIT_MS, next),
            next => putObject(bucket, key, objectSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize * 2, 0, 2);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(objectSize * 2, 0, 3);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multipartUpload in a versioned bucket', done => {
        const bucket = 'bucket8';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '8.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts, 0, 1);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multipartUpload overwrite in a versioned bucket', done => {
        const bucket = 'bucket9';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '9.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts * 2, 0, 2);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiPartUpload overwrite', done => {
        const bucket = 'bucket10';
        const partSize = 1024 * 1024 * 6;
        const parts = 2;
        const key = '10.txt';
        async.series([
            next => createBucket(bucket, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => objectMPU(bucket, key, parts, partSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(partSize * parts, 0, 1);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
    it('should set metrics for multiObjectDelete in a versioned bucket', done => {
        const bucket = 'bucket11';
        const objectSize = 1024 * 1024;
        const obj1Size = objectSize * 2;
        const obj2Size = objectSize * 1;
        const key1 = '1.txt';
        const key2 = '2.txt';
        async.series([
            next => createBucket(bucket, next),
            next => enableVersioning(bucket, true, next),
            next => putObject(bucket, key1, obj1Size, next),
            next => wait(WAIT_MS, next),
            next => putObject(bucket, key2, obj2Size, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 2);
                next();
            }),
            next => deleteObjects(bucket, [key1, key2], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(obj1Size + obj2Size, 0, 4);
                next();
            }),
            next => removeVersions([bucket], next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });

    it('should not push a metric for a filtered bucket', done => {
        const bucket = 'utapi-event-filter-deny-bucket';
        const objSize = 2 * 1024 * 1024;
        const key = '1.txt';
        async.series([
            next => createBucket(bucket, next),
            next => putObject(bucket, key, objSize, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteObject(bucket, key, next),
            next => wait(WAIT_MS, () => {
                checkMetrics(0, 0, 0);
                next();
            }),
            next => deleteBucket(bucket, next),
        ], done);
    });
});
