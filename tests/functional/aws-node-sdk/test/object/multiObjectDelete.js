import assert from 'assert';

import config from '../../../../../lib/Config';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

const userBucketUtility = new BucketUtility('userBart', {});
const userS3 = userBucketUtility.s3;

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
    for (let i = 1; i < (size + 1); i ++) {
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
        return s3.createBucketAsync({ Bucket: bucketName })
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            const createFirstHalfObjects = [];
            for (let i = 1; i < 500; i ++) {
                createFirstHalfObjects.push(s3.putObjectAsync({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                }));
            }
            return Promise.all(createFirstHalfObjects)
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        })
        .then(() => {
            const createSecondHalfObjects = [];
            for (let i = 501; i < 1001; i ++) {
                createSecondHalfObjects.push(s3.putObjectAsync({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                }));
            }
            return Promise.all(createSecondHalfObjects)
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    afterEach(() => s3.deleteBucketAsync({ Bucket: bucketName }));

    it('should batch delete 1000 objects', () => {
        const objects = createObjectsList(1000);
        return s3.deleteObjectsAsync({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).then(res => {
            assert.strictEqual(res.Deleted.length, 1000);
            // order of returned objects not sorted
            assert.deepStrictEqual(sortList(res.Deleted), sortList(objects));
            assert.strictEqual(res.Errors.length, 0);
        }).catch(err => {
            checkNoError(err);
        });
    });

    it('should batch delete 1000 objects quietly', () => {
        const objects = createObjectsList(1000);
        return s3.deleteObjectsAsync({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: true,
            },
        }).then(res => {
            assert.strictEqual(res.Deleted.length, 0);
            assert.strictEqual(res.Errors.length, 0);
        }).catch(err => {
            checkNoError(err);
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
            return s3.createBucketAsync({ Bucket: bucketName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => s3.deleteBucketAsync({ Bucket: bucketName }));

        it('should return error if request deletion of more than 1000 objects',
            () => {
                const objects = createObjectsList(1001);
                return s3.deleteObjectsAsync({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).catch(err => {
                    checkError(err, 'MalformedXML');
                });
            });

        it('should return no error if try to delete non-existent objects',
            () => {
                const objects = createObjectsList(1000);
                return s3.deleteObjectsAsync({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).then(res => {
                    assert.strictEqual(res.Deleted.length, 1000);
                    assert.strictEqual(res.Errors.length, 0);
                }).catch(err => {
                    checkNoError(err);
                });
            });

        it('should return error if no such bucket', () => {
            const objects = createObjectsList(1);
            return s3.deleteObjectsAsync({
                Bucket: 'nosuchbucket2323292093',
                Delete: {
                    Objects: objects,
                },
            }).catch(err => {
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
        return s3.createBucketAsync({ Bucket: bucketName })
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            for (let i = 1; i < 501; i ++) {
                createObjects.push(s3.putObjectAsync({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                }));
            }
            return Promise.all(createObjects)
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    after(() => s3.deleteBucketAsync({ Bucket: bucketName }));

    it('should return access denied error for each object where no acl ' +
        'permission', () => {
        const objects = createObjectsList(500);
        const errorList = createObjectsList(500);
        errorList.forEach(obj => {
            const item = obj;
            item.Code = 'AccessDenied';
            item.Message = 'Access Denied';
        });
        return otherAccountS3.deleteObjectsAsync({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).then(res => {
            assert.strictEqual(res.Deleted.length, 0);
            assert.deepStrictEqual(sortList(res.Errors), sortList(errorList));
            assert.strictEqual(res.Errors.length, 500);
        }).catch(err => {
            checkNoError(err);
        });
    });


    it('should batch delete objects where requester has permission', () => {
        // if test run with file or mem backend, test user access for
        // in memory implementation (which should grant user access).
        // if using distributed backend, test with account since need
        // policy authorizaing user for user to have access.
        // tests of user with a distributed backend are in integration.
        const requester = config.backends.auth === 'mem' ? userS3 : s3;
        const objects = createObjectsList(500);
        return requester.deleteObjectsAsync({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).then(res => {
            assert.strictEqual(res.Deleted.length, 500);
            assert.strictEqual(res.Errors.length, 0);
        }).catch(err => {
            checkNoError(err);
        });
    });
});
