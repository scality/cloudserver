const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutBucketAclCommand,
    ListObjectsV2Command,
    PutObjectAclCommand,
    GetObjectCommand,
    GetBucketAclCommand,
    GetObjectAclCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const constants = require('../../../../../constants');

const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;
const describeSkipIfE2E = process.env.S3_END_TO_END ? describe.skip : describe;

withV4(sigCfg => {
    const ownerAccountBucketUtil = new BucketUtility('default', sigCfg);
    const otherAccountBucketUtil = new BucketUtility('lisa', sigCfg);
    const s3 = ownerAccountBucketUtil.s3;
    const testBucket = 'predefined-groups-bucket';
    const testKey = '0.txt';
    const ownerObjKey = 'account.txt';
    const testBody = '000';

    function awsRequest(auth, Operation, params) {
        if (auth) {
            return otherAccountBucketUtil.s3.send(new Operation(params));
        } else {
            const command = new Operation(params);
            
            // Create unsigned client
            const unsignedClient = new BucketUtility('default', {
                ...sigCfg,
                credentials: { accessKeyId: '', secretAccessKey: '' },
                forcePathStyle: true,
                signer: { sign: async request => request },
            });

            // Replace awsAuthMiddleware with a no-op middleware to skip signing
            unsignedClient.s3.middlewareStack.use({
                name: 'noAuthMiddleware',
                step: 'serialize',
                priority: 'high',
                override: true,
                tags: ['S3', 'NO_AUTH'],
                applyToStack: stack => {
                    stack.addRelativeTo(
                        next => async args => {
                            // Ensure no auth headers are added
                            if (args.request && args.request.headers) {
                                // eslint-disable-next-line no-param-reassign
                                delete args.request.headers['x-amz-date'];
                                // eslint-disable-next-line no-param-reassign
                                delete args.request.headers['x-amz-content-sha256'];
                                // eslint-disable-next-line no-param-reassign
                                delete args.request.headers['x-amz-security-token'];
                                // eslint-disable-next-line no-param-reassign
                                delete args.request.headers['authorization'];
                            }
                            return next(args);
                        },
                        {
                            name: 'noAuthMiddleware',
                            step: 'serialize',
                            priority: 'high',
                            before: 'awsAuthMiddleware',
                        }
                    );
                }
            });
            return unsignedClient.s3.send(command);
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
            try {
                assert.notStrictEqual(err, null);
                assert.strictEqual(err.$metadata?.httpStatusCode, 403);
                assert.strictEqual(err.Code, 'AccessDenied');
                done();
            } catch (assertError) {
                done(assertError);
            }
        };
    }

    // tests for authenticated user(signed) and anonymous user(unsigned)
    [true, false].forEach(auth => {
        const authType = auth ? 'authenticated' : 'unauthenticated';
        const grantUri = `uri=${auth ? constants.allAuthedUsersId : constants.publicId}`;

        // TODO fix flakiness on E2E and re-enable, see CLDSRV-254
        describeSkipIfE2E('PUT Bucket ACL using predefined groups - ' +
            `${authType} request`, () => {
            const aclParam = {
                Bucket: testBucket,
                ACL: 'private',
            };

            beforeEach(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: testBucket }));
                await s3.send(new PutObjectCommand({
                    Bucket: testBucket,
                    Body: testBody,
                    Key: ownerObjKey,
                }));
            });

            afterEach(async () => {
                await ownerAccountBucketUtil.empty(testBucket);
                await ownerAccountBucketUtil.deleteOne(testBucket);
            });

            it('should grant read access', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantRead: grantUri,
                }))
                    .then(() => awsRequest(auth, ListObjectsV2Command, { Bucket: testBucket })));

            it('should grant read access with grant-full-control', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }))
                    .then(() => awsRequest(auth, ListObjectsV2Command, { Bucket: testBucket })));

            it('should not grant read access', done => {
                s3.send(new PutBucketAclCommand(aclParam))
                    .then(() => awsRequest(auth, ListObjectsV2Command, { Bucket: testBucket }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
                // Don't return the promise!
            });

            it('should grant write access', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }))
                    .then(() => awsRequest(auth, PutObjectCommand, {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    })));

            it('should grant write access with grant-full-control', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }))
                    .then(() => awsRequest(auth, PutObjectCommand, {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    })));

            it('should not grant write access', done => {
                s3.send(new PutBucketAclCommand(aclParam))
                    .then(() => awsRequest(auth, PutObjectCommand, {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: testKey,
                    }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });

            itSkipIfE2E('should grant write access on an object not owned by the grantee', 
                () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }))
                    .then(() => awsRequest(auth, PutObjectCommand, {
                        Bucket: testBucket,
                        Body: testBody,
                        Key: ownerObjKey,
                    })));

            it(`should ${auth ? '' : 'not '}delete object not owned by the grantee`, done => {
                s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantWrite: grantUri,
                }))
                    .then(() => awsRequest(auth, DeleteObjectCommand, {
                        Bucket: testBucket,
                        Key: ownerObjKey,
                    }))
                    .then(() => {
                        if (auth) {
                            done();
                        } else {
                            done(new Error('Expected failure'));
                        }
                    })
                    .catch(err => {
                        if (auth) {
                            cbNoError(done)(err);
                        } else {
                            cbWithError(done)(err);
                        }
                    });
            });

            it('should read bucket acl', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantReadACP: grantUri,
                }))
                    .then(() => awsRequest(auth, GetBucketAclCommand, { Bucket: testBucket })));

            it('should read bucket acl with grant-full-control', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }))
                    .then(() => awsRequest(auth, GetBucketAclCommand, { Bucket: testBucket })));

            it('should not read bucket acl', done => {
                s3.send(new PutBucketAclCommand(aclParam))
                    .then(() => awsRequest(auth, GetBucketAclCommand, { Bucket: testBucket }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });

            it('should write bucket acl', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantWriteACP: grantUri,
                }))
                    .then(() => awsRequest(auth, PutBucketAclCommand, {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.publicId}`,
                    })));

            it('should write bucket acl with grant-full-control', () => s3.send(new PutBucketAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                }))
                    .then(() => awsRequest(auth, PutBucketAclCommand, {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.publicId}`,
                    })));

            it('should not write bucket acl', done => {
                s3.send(new PutBucketAclCommand(aclParam))
                    .then(() => awsRequest(auth, PutBucketAclCommand, {
                        Bucket: testBucket,
                        GrantReadACP: `uri=${constants.allAuthedUsersId}`,
                    }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });
        });

        describe(`PUT Object ACL using predefined groups - ${authType} request`, () => {
            const aclParam = {
                Bucket: testBucket,
                Key: testKey,
                ACL: 'private',
            };

            beforeEach(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: testBucket }));
                await s3.send(new PutObjectCommand({
                    Bucket: testBucket,
                    Body: testBody,
                    Key: testKey,
                }));
            });

            afterEach(async () => {
                await ownerAccountBucketUtil.empty(testBucket);
                await ownerAccountBucketUtil.deleteOne(testBucket);
            });

            it('should grant read access', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantRead: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, GetObjectCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    })));

            it('should grant read access with grant-full-control', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, GetObjectCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    })));

            it('should not grant read access', done => {
                s3.send(new PutObjectAclCommand(aclParam))
                    .then(() => awsRequest(auth, GetObjectCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });

            it('should read object acl', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantReadACP: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, GetObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    })));

            it('should read object acl with grant-full-control', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, GetObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    })));

            it('should not read object acl', done => {
                s3.send(new PutObjectAclCommand(aclParam))
                    .then(() => awsRequest(auth, GetObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                    }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });

            it('should write object acl', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantWriteACP: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, PutObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: grantUri,
                    })));

            it('should write object acl with grant-full-control', () => s3.send(new PutObjectAclCommand({
                    Bucket: testBucket,
                    GrantFullControl: grantUri,
                    Key: testKey,
                }))
                    .then(() => awsRequest(auth, PutObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: `uri=${constants.publicId}`,
                    })));

            it('should not write object acl', done => {
                s3.send(new PutObjectAclCommand(aclParam))
                    .then(() => awsRequest(auth, PutObjectAclCommand, {
                        Bucket: testBucket,
                        Key: testKey,
                        GrantReadACP: `uri=${constants.allAuthedUsersId}`,
                    }))
                    .then(() => done(new Error('Expected failure')))
                    .catch(cbWithError(done));
            });
        });
    });
});
