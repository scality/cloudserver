const { promisify } = require('util');
const assert = require('assert');
const moment = require('moment');
const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectsCommand,
    DeleteBucketCommand,
    PutObjectLockConfigurationCommand,
    PutObjectLegalHoldCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;
const changeLockPromise = promisify(changeObjectLock);

const bucketName = 'multi-object-delete-234-634';
const key = 'key';

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function sortList(list) {
    return list.sort((a, b) => {
        // Handle both string arrays and object arrays
        const keyA = typeof a === 'string' ? a : a.Key;
        const keyB = typeof b === 'string' ? b : b.Key;
        
        // Extract numeric part from keys like 'key1', 'key2', 'key10', etc.
        const getNumber = key => parseInt(key.replace(/^key/, ''), 10);
        const numA = getNumber(keyA);
        const numB = getNumber(keyB);
        return numA - numB;
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
        objects.forEach((obj, index) => {
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

    beforeEach(async () => {
        bucketUtil = new BucketUtility('default', {
            signatureVersion: 'v4',
        });
        s3 = bucketUtil.s3;
        try {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            const objects = [];
            for (let i = 1; i < 1001; i++) {
                objects.push(`${key}${i}`);
            }
            const parallel = 20;
            const queued = [];
            const putObjectWithLimit = async key => {
                while (queued.length >= parallel) {
                    await Promise.race(queued);
                    queued.splice(0, queued.findIndex(p => p === queued[0]) + 1);
                }
                const result = s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: key,
                    Body: 'somebody',
                }));
                queued.push(result);
                return result;
            };
            const putPromises = objects.map(key => putObjectWithLimit(key));
            await Promise.all(putPromises);
        } catch (err) {
            process.stdout.write(`Error creating objects: ${err}\n`);
            throw err;
        }
    });

    afterEach(async () => {
        await bucketUtil.empty(bucketName);
        await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should batch delete 1000 objects', done => {
        const objects = createObjectsList(1000);
        s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        })).then(res => {
            if (this.httpResponse?.body?.toString()
                    .indexOf('<?xml version="1.0"') === -1) {
                return done('S3C-2642: should have included xml declaration');
            }
            assert.strictEqual(res.Deleted.length, 1000);
            assert.deepStrictEqual(sortList(res.Deleted.map(obj => obj.Key)), sortList(objects.map(obj => obj.Key)));
            return done();
        }).catch(err => done(err));
    });

    it('should batch delete 1000 objects quietly', done => {
        const objects = createObjectsList(1000);
        s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: true,
            },
        })).then(res => {
            if (this.httpResponse?.body?.toString()
                    .indexOf('<?xml version="1.0"') === -1) {
                return done('S3C-2642: should have included xml declaration');
            }
            assert.strictEqual(res.Deleted, undefined);
            return done();
        }).catch(err => done(err));
    });
});

describe('Multi-Object Delete Error Responses', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.send(new CreateBucketCommand({ Bucket: bucketName }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(async () => {
            await bucketUtil.empty(bucketName);
            await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
        });

        it('should return error if request deletion of more than 1000 objects',
            () => {
                const objects = createObjectsList(1001);
                return s3.send(new DeleteObjectsCommand({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                })).catch(err => {
                    checkError(err, 'MalformedXML', 400);
                });
            });

        it('should return error if request deletion of 0 objects',
            () => {
                const objects = createObjectsList(0);
                return s3.send(new DeleteObjectsCommand({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                })).catch(err => {
                    checkError(err, 'MalformedXML', 400);
                });
            });

        it('should return no error if try to delete non-existent objects',
            () => {
                const objects = createObjectsList(1000);
                return s3.send(new DeleteObjectsCommand({
                    Bucket: bucketName,
                    Delete: {
                        Objects: objects,
                    },
                })).then(res => {
                    assert.strictEqual(res.Deleted.length, 1000);
                }).catch(err => {
                    checkNoError(err);
                });
            });

        it('should return error if no such bucket', () => {
            const objects = createObjectsList(1);
            return s3.send(new DeleteObjectsCommand({
                Bucket: 'nosuchbucket2323292093',
                Delete: {
                    Objects: objects,
                },
            })).catch(err => {
                checkError(err, 'NoSuchBucket', 404);
            });
        });
    });
});

describe('Multi-Object Delete Access', function access() {
    this.timeout(360000);
    let bucketUtil;
    let s3;

    before(() => {
        bucketUtil = new BucketUtility('default', {
            signatureVersion: 'v4',
        });
        s3 = bucketUtil.s3;
        return s3.send(new CreateBucketCommand({ Bucket: bucketName }))
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            const createObjects = [];
            for (let i = 1; i < 501; i++) {
                createObjects.push(s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                })));
            }
            return Promise.all(createObjects)
            .catch(err => {
                process.stdout.write(`Error creating objects: ${err}\n`);
                throw err;
            });
        });
    });

    after(async () => {
        await bucketUtil.empty(bucketName);
        await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should return access denied error for each object where no acl ' +
        'permission', () => {
        const objects = createObjectsList(500);
        const errorList = createObjectsList(500);
        errorList.forEach(obj => {
            const item = obj;
            item.Code = 'AccessDenied';
            item.Message = 'Access Denied';
        });
        return otherAccountS3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        })).then(res => {
            assert.strictEqual(res.Deleted, undefined);
            assert.strictEqual(res.Errors.length, 500);
            assert.deepStrictEqual(sortList(res.Errors), sortList(errorList));
        }).catch(err => {
            checkNoError(err);
        });
    });

    it('should batch delete objects where requester has permission', () => {
        const objects = createObjectsList(500);
        return s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        })).then(res => {
            assert.strictEqual(res.Deleted.length, 500);
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
        return s3.send(new CreateBucketCommand({
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        }))
        .then(() => s3.send(new PutObjectLockConfigurationCommand({
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
        })))
        .catch(err => {
            process.stdout.write(`Error creating bucket: ${err}\n`);
            throw err;
        })
        .then(() => {
            for (let i = 1; i < 6; i++) {
                createObjects.push(s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: `${key}${i}`,
                    Body: 'somebody',
                })));
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

    after(async () => {
        await bucketUtil.empty(bucketName);
        await s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should not delete locked objects', () => {
        const objects = createObjectsList(5, versionIds);
        return s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        })).then(res => {
            assert.strictEqual(res.Errors.length, 5);
            res.Errors.forEach(err => assert.strictEqual(err.Code, 'AccessDenied'));
        });
    });

    it('should not delete locked objects with GOVERNANCE ' +
        'retention mode and bypass header when object is legal hold enabled', () => {
        const objects = createObjectsList(5, versionIds);
        const putObjectLegalHolds = [];
        for (let i = 1; i < 6; i++) {
            putObjectLegalHolds.push(s3.send(new PutObjectLegalHoldCommand({
                Bucket: bucketName,
                Key: `${key}${i}`,
                LegalHold: {
                    Status: 'ON',
                },
            })));
        }
        return Promise.all(putObjectLegalHolds)
            .then(() => s3.send(new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: objects,
                    Quiet: false,
                },
                BypassGovernanceRetention: true,
            }))).then(res => {
                assert.strictEqual(res.Errors.length, 5);
                res.Errors.forEach(err => assert.strictEqual(err.Code, 'AccessDenied'));
            });
    });

    it('should delete locked objects after retention period has expired', () => {
        const objects = createObjectsList(5, versionIds);
        const objectsCopy = JSON.parse(JSON.stringify(objects));
        for (let i = 0; i < objectsCopy.length; i++) {
            objectsCopy[i].key = objectsCopy[i].Key;
            objectsCopy[i].versionId = objectsCopy[i].VersionId;
            objectsCopy[i].bucket = bucketName;
            delete objectsCopy[i].Key;
            delete objectsCopy[i].VersionId;
        }
        const newRetention = {
            mode: 'GOVERNANCE',
            date: moment().subtract(10, 'days').toISOString(),
        };
        return changeLockPromise(objectsCopy, newRetention)
        .then(() => s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
        }))).then(res => {
            assert.strictEqual(res.Deleted.length, 5);
        }).catch(err => {
            checkNoError(err);
        });
    });

    it('should delete locked objects with GOVERNANCE ' +
        'retention mode and bypass header', () => {
        const objects = createObjectsList(5, versionIds);
        return s3.send(new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
                Objects: objects,
                Quiet: false,
            },
            BypassGovernanceRetention: true,
        })).then(res => {
            assert.strictEqual(res.Deleted.length, 5);
            assert.strictEqual(res.Errors, undefined);
        }).catch(err => {
            checkNoError(err);
        });
    });
});
