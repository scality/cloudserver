const assert = require('assert');
const AWS = require('aws-sdk');
const { errors } = require('arsenal');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const constants = require('../../../../../constants');
const { VALIDATE_CREDENTIALS, SIGN } = AWS.EventListeners.Core;

withV4(sigCfg => {
    const ownerAccountBucketUtil = new BucketUtility('default', sigCfg);
    const otherAccountBucketUtil = new BucketUtility('lisa', sigCfg);
    const s3 = ownerAccountBucketUtil.s3;

    const testBucket = 'predefined-groups-bucket';
    const testKey = '0.txt';
    const ownerObjKey = 'account.txt';
    const testBody = '000';

    function awsRequest(auth, operation, params, callback) {
        if (auth) {
            otherAccountBucketUtil.s3[operation](params, callback);
        } else {
            const bucketUtil = new BucketUtility('default', sigCfg);
            const request = bucketUtil.s3[operation](params);
            request.removeListener('validate', VALIDATE_CREDENTIALS);
            request.removeListener('sign', SIGN);
            request.send(callback);
        }
    }

    function cbNoError(done) {
        return err => {
            assert.ifError(err);
            done();
        };
    }

    function cbWithError(done) {
        return err => {
            assert.notStrictEqual(err, null);
            assert.strictEqual(err.statusCode, errors.AccessDenied.code);
            done();
        };
    }

    // tests for authenticated user(signed) and anonymous user(unsigned)
    [true, false].forEach(auth => {
        const authType = auth ? 'authenticated' : 'unauthenticated';
        const grantUri = `uri=${auth ?
            constants.allAuthedUsersId : constants.publicId}`;

        describe('PUT Bucket ACL using predefined groups - ' +
            `${authType} request`, () => {
            const aclParam = {
                Bucket: testBucket,
                ACL: 'private',
            };

            beforeEach(done => s3.createBucket({
                Bucket: testBucket,
            }, err => {
                assert.ifError(err);
                return s3.putObject({
                    Bucket: testBucket,
                    Body: testBody,
                    Key: ownerObjKey,
                }, done);
            }));
            afterEach(() => ownerAccountBucketUtil.empty(testBucket)
                .then(() => ownerAccountBucketUtil.deleteOne(testBucket)));

            it('should grant read access', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantRead: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'listObjects', param, cbNoError(done));
                });
            });

            it('should grant read access with grant-full-control', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'listObjects', param, cbNoError(done));
                });
            });

            it('should not grant read access', done => {
                s3.putBucketAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'listObjects', param, cbWithError(done));
                });
            });

            it('should grant write access', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    };
                    awsRequest(auth, 'putObject', param, cbNoError(done));
                });
            });

            it('should grant write access with ' +
                'grant-full-control', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    };
                    awsRequest(auth, 'putObject', param, cbNoError(done));
                });
            });

            it('should not grant write access', done => {
                s3.putBucketAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    };
                    awsRequest(auth, 'putObject', param, cbWithError(done));
                });
            });

            it('should grant write access on an object not owned ' +
                'by the grantee', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: ownerObjKey,
                    };
                    awsRequest(auth, 'putObject', param, cbNoError(done));
                });
            });

            it(`should ${auth ? '' : 'not '}delete object not owned by the` +
            'grantee', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: ownerObjKey,
                    };
                    awsRequest(auth, 'deleteObject', param, err => {
                        if (auth) {
                            assert.ifError(err);
                        } else {
                            assert.notStrictEqual(err, null);
                            assert.strictEqual(
                                err.statusCode,
                                errors.AccessDenied.code
                            );
                        }
                        done();
                    });
                });
            });

            it('should read bucket acl', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantReadACP: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'getBucketAcl', param, cbNoError(done));
                });
            });

            it('should read bucket acl with grant-full-control', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'getBucketAcl', param, cbNoError(done));
                });
            });

            it('should not read bucket acl', done => {
                s3.putBucketAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = { Bucket: testBucket };
                    awsRequest(auth, 'getBucketAcl', param, cbWithError(done));
                });
            });

            it('should write bucket acl', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantWriteACP: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.publicId}`,
                    };
                    awsRequest(auth, 'putBucketAcl', param, cbNoError(done));
                });
            });

            it('should write bucket acl with grant-full-control', done => {
                s3.putBucketAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.publicId}`,
                    };
                    awsRequest(auth, 'putBucketAcl', param, cbNoError(done));
                });
            });

            it('should not write bucket acl', done => {
                s3.putBucketAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.allAuthedUsersId}`,
                    };
                    awsRequest(auth, 'putBucketAcl', param, cbWithError(done));
                });
            });
        });

        describe('PUT Object ACL using predefined groups - ' +
            `${authType} request`, () => {
            const aclParam = {
                Bucket: testBucket,
                Key: testKey,
                ACL: 'private',
            };
            beforeEach(done => s3.createBucket({
                Bucket: testBucket,
            }, err => {
                assert.ifError(err);
                return s3.putObject({
                    Bucket: testBucket,
                    Body: testBody,
                    Key: testKey,
                }, done);
            }));
            afterEach(() => ownerAccountBucketUtil.empty(testBucket)
                .then(() => ownerAccountBucketUtil.deleteOne(testBucket)));

            it('should grant read access', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantRead: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObject', param, cbNoError(done));
                });
            });

            it('should grant read access with grant-full-control', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObject', param, cbNoError(done));
                });
            });

            it('should not grant read access', done => {
                s3.putObjectAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObject', param, cbWithError(done));
                });
            });

            it('should read object acl', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantReadACP: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObjectAcl', param, cbNoError(done));
                });
            });

            it('should read object acl with grant-full-control', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObjectAcl', param, cbNoError(done));
                });
            });

            it('should not read object acl', done => {
                s3.putObjectAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                    };
                    awsRequest(auth, 'getObjectAcl', param, cbWithError(done));
                });
            });

            it('should write object acl', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantWriteACP: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: grantUri,
                    };
                    awsRequest(auth, 'putObjectAcl', param, cbNoError(done));
                });
            });

            it('should write object acl with grant-full-control', done => {
                s3.putObjectAcl({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: `uri=${constants.publicId}`,
                    };
                    awsRequest(auth, 'putObjectAcl', param, cbNoError(done));
                });
            });

            it('should not write object acl', done => {
                s3.putObjectAcl(aclParam, err => {
                    assert.ifError(err);
                    const param = {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: `uri=${constants.allAuthedUsersId}`,
                    };
                    awsRequest(auth, 'putObjectAcl', param, cbWithError(done));
                });
            });
        });
    });
});
