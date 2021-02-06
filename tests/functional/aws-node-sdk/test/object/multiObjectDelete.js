const assert = require('assert');
const Promise = require('bluebird');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

const bucketName = 'multi-object-delete-234-634';
const key = 'key';

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function checkError(err, code) {
    assert.notEqual(err, null, 'Expected failure but got success');
    assert.strictEqual(err.code, code);
}

function sortList(list) {
    return list.sort((a, b) => {
        if (a.Key > b.Key) {
            return 1;
        }
        if (a.Key < b.Key) {
            return -1;
        }
        return 0;
    });
}

function createObjectsList(size) {
    const objects = [];
    for (let i = 1; i < (size + 1); i++) {
        objects.push({
            Key: `${key}${i}`,
        });
    }
    return objects;
}

describe('Multi-Object Delete Success', function success() {
    this.timeout(360000);
    let bucketUtil;
    let s3;

    beforeEach(() => {
        bucketUtil = new BucketUtility('default', {
            signatureVersion: 'v4',
        });
        s3 = bucketUtil.s3;
        return s3.createBucket({ Bucket: bucketName }).promise()
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            const objects = [];
            for (let i = 1; i < 1001; i++) {
                objects.push(`${key}${i}`);
            }
            const queued = [];
            const parallel = 20;
            const putPromises = objects.map(key => {
                const mustComplete = Math.max(0, queued.length - parallel + 1);
                const result = Promise.some(queued, mustComplete).then(() =>
                    s3.putObject({
                        Bucket: bucketName,
                        Key: key,
                        Body: 'somebody',
                    }).promise()
                );
                queued.push(result);
                return result;
            });
            return Promise.all(putPromises).catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    afterEach(() => s3.deleteBucket({ Bucket: bucketName }).promise());

    it('should batch delete 1000 objects', done => {
        const objects = createObjectsList(1000);
        s3.deleteObjects({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }, function result(err, res) {
            checkNoError(err);
            if (this.httpResponse.body.toString()
                    .indexOf('<?xml version="1.0"') === -1) {
                return done('S3C-2642: should have included xml declaration');
            }
            assert.strictEqual(res.Deleted.length, 1000);
            // order of returned objects not sorted
            assert.deepStrictEqual(sortList(res.Deleted), sortList(objects));
            assert.strictEqual(res.Errors.length, 0);
            return done();
        });
    });

    it('should batch delete 1000 objects quietly', done => {
        const objects = createObjectsList(1000);
        s3.deleteObjects({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: true,
            },
        }, function result(err, res) {
            checkNoError(err);
            if (this.httpResponse.body.toString()
                    .indexOf('<?xml version="1.0"') === -1) {
                return done('S3C-2642: should have included xml declaration');
            }
            assert.strictEqual(res.Deleted.length, 0);
            assert.strictEqual(res.Errors.length, 0);
            return done();
        });
    });
});

describe('Multi-Object Delete Error Responses', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucket({ Bucket: bucketName }).promise()
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => s3.deleteBucket({ Bucket: bucketName }).promise());

        it('should return error if request deletion of more than 1000 objects',
            () => {
                const objects = createObjectsList(1001);
                return s3.deleteObjects({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).promise().catch(err => {
                    checkError(err, 'MalformedXML');
                });
            });

        it('should return error if request deletion of 0 objects',
            () => {
                const objects = createObjectsList(0);
                return s3.deleteObjects({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).promise().catch(err => {
                    checkError(err, 'MalformedXML');
                });
            });

        it('should return no error if try to delete non-existent objects',
            () => {
                const objects = createObjectsList(1000);
                return s3.deleteObjects({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).promise().then(res => {
                    assert.strictEqual(res.Deleted.length, 1000);
                    assert.strictEqual(res.Errors.length, 0);
                }).catch(err => {
                    checkNoError(err);
                });
            });

        it('should return error if no such bucket', () => {
            const objects = createObjectsList(1);
            return s3.deleteObjects({
                Bucket: 'nosuchbucket2323292093',
                Delete: {
                    Objects: objects,
                },
            }).promise().catch(err => {
                checkError(err, 'NoSuchBucket');
            });
        });
    });
});

describe('Multi-Object Delete Access', function access() {
    this.timeout(360000);
    let bucketUtil;
    let s3;

    before(() => {
        const createObjects = [];
        bucketUtil = new BucketUtility('default', {
            signatureVersion: 'v4',
        });
        s3 = bucketUtil.s3;
        return s3.createBucket({ Bucket: bucketName }).promise()
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            for (let i = 1; i < 501; i++) {
                createObjects.push(s3.putObject({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                }).promise());
            }
            return Promise.all(createObjects)
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    after(() => s3.deleteBucket({ Bucket: bucketName }).promise());

    it('should return access denied error for each object where no acl ' +
        'permission', () => {
        const objects = createObjectsList(500);
        const errorList = createObjectsList(500);
        errorList.forEach(obj => {
            const item = obj;
            item.Code = 'AccessDenied';
            item.Message = 'Access Denied';
        });
        return otherAccountS3.deleteObjects({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).promise().then(res => {
            assert.strictEqual(res.Deleted.length, 0);
            assert.deepStrictEqual(sortList(res.Errors), sortList(errorList));
            assert.strictEqual(res.Errors.length, 500);
        }).catch(err => {
            checkNoError(err);
        });
    });


    it('should batch delete objects where requester has permission', () => {
        const objects = createObjectsList(500);
        return s3.deleteObjects({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).promise().then(res => {
            assert.strictEqual(res.Deleted.length, 500);
            assert.strictEqual(res.Errors.length, 0);
        }).catch(err => {
            checkNoError(err);
        });
    });
});

