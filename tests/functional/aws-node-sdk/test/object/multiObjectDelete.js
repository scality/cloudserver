const assert = require('assert');
const moment = require('moment');
const Promise = require('bluebird');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const changeObjectLock = require('../../lib/utility/objectLock-util');

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;
const changeLockPromise = Promise.promisify(changeObjectLock);

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

function createObjectsList(size, versionIds) {
    const objects = [];
    for (let i = 1; i < (size + 1); i++) {
        objects.push({
            Key: `${key}${i}`,
        });
    }
    if (versionIds) {
        objects.map((obj, index) => {
            // eslint-disable-next-line no-param-reassign
            obj.VersionId = versionIds[index];
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
        return s3.createBucketPromise({ Bucket: bucketName })
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
                    s3.putObjectPromise({
                        Bucket: bucketName,
                        Key: key,
                        Body: 'somebody',
                    })
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

    afterEach(() => s3.deleteBucketPromise({ Bucket: bucketName }));

    it('should batch delete 1000 objects', () => {
        const objects = createObjectsList(1000);
        return s3.deleteObjectsPromise({
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
        return s3.deleteObjectsPromise({
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
            return s3.createBucketPromise({ Bucket: bucketName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => s3.deleteBucketPromise({ Bucket: bucketName }));

        it('should return error if request deletion of more than 1000 objects',
            () => {
                const objects = createObjectsList(1001);
                return s3.deleteObjectsPromise({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                }).catch(err => {
                    checkError(err, 'MalformedXML');
                });
            });

        it('should return error if request deletion of 0 objects',
            () => {
                const objects = createObjectsList(0);
                return s3.deleteObjectsPromise({
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
                return s3.deleteObjectsPromise({
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
            return s3.deleteObjectsPromise({
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
        return s3.createBucketPromise({ Bucket: bucketName })
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            for (let i = 1; i < 501; i++) {
                createObjects.push(s3.putObjectPromise({
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

    after(() => s3.deleteBucketPromise({ Bucket: bucketName }));

    it('should return access denied error for each object where no acl ' +
        'permission', () => {
        const objects = createObjectsList(500);
        const errorList = createObjectsList(500);
        errorList.forEach(obj => {
            const item = obj;
            item.Code = 'AccessDenied';
            item.Message = 'Access Denied';
        });
        return otherAccountS3.deleteObjectsPromise({
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
        const objects = createObjectsList(500);
        return s3.deleteObjectsPromise({
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

describe('Multi-Object Delete with Object Lock', () => {
    let bucketUtil;
    let s3;
    const versionIds = [];

    before(() => {
        const createObjects = [];
        bucketUtil = new BucketUtility('default', {
            signatureVersion: 'v4',
        });
        s3 = bucketUtil.s3;
        return s3.createBucketPromise({
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        })
        .then(() => s3.putObjectLockConfigurationPromise({
            Bucket: bucketName,
            ObjectLockConfiguration: {
                ObjectLockEnabled: 'Enabled',
                Rule: {
                    DefaultRetention: {
                        Days: 1,
                        Mode: 'GOVERNANCE',
                    },
                },
            },
        }))
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            for (let i = 1; i < 6; i++) {
                createObjects.push(s3.putObjectPromise({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                }));
            }
            return Promise.all(createObjects)
            .then(res => {
                res.forEach(r => {
                    versionIds.push(r.VersionId);
                });
            })
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    after(() => s3.deleteBucketPromise({ Bucket: bucketName }));

    it('should not delete locked objects', () => {
        const objects = createObjectsList(5, versionIds);
        return s3.deleteObjectsPromise({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }).then(res => {
            assert.strictEqual(res.Errors.length, 5);
            res.Errors.forEach(err => assert.strictEqual(err.Code, 'AccessDenied'));
        });
    });

    it('should delete locked objects after retention period has expired', () => {
        const objects = createObjectsList(5, versionIds);
        const objectsCopy = JSON.parse(JSON.stringify(objects));
        for(let i = 0; i < objectsCopy.length; i++){
            objectsCopy[i].key = objectsCopy[i].Key;
            objectsCopy[i].versionId = objectsCopy[i].VersionId;
            objectsCopy[i].bucket = bucketName;
            delete objectsCopy[i].Key;
            delete objectsCopy[i].VersionId;
        }
        const newRetention = {
            retention: {
                mode: 'GOVERNANCE',
                retainUntilDate: moment().subtract(10, 'days').toISOString(),
            },
        };
        return changeLockPromise(objectsCopy, newRetention)
        .then(() => s3.deleteObjectsPromise({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        })).then(res => {
            assert.strictEqual(res.Deleted.length, 5);
            assert.strictEqual(res.Errors.length, 0);
        }).catch(err => {
            checkNoError(err);
        });
    });
});
