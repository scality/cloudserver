const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, genBucketName, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    HeadObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';

describe('GCP: Object', function testSuite() {
    this.timeout(180000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);
    const bucketName = genBucketName('object');

    before(async () => {
        await gcpRetry(
            gcpClient,
            new CreateBucketCommand({ Bucket: bucketName }),
        );
    });

    after(async () => {
        await gcpRetry(gcpClient, new DeleteBucketCommand({ Bucket: bucketName }));
    });

    async function setupExistingObject(test) {
        /* eslint-disable no-param-reassign */
        test.key = `somekey-${genUniqID()}`;
        const res = await gcpClient.send(new PutObjectCommand({
            Bucket: bucketName,
            Key: test.key,
        }));
        test.uploadId = res.VersionId;
        test.ETag = res.ETag;
        /* eslint-enable no-param-reassign */
    }

    async function cleanupObject(test) {
        if (!test.key) {
            return;
        }
        await gcpClient.send(new DeleteObjectCommand({
            Bucket: bucketName,
            Key: test.key,
        }));
    }

    describe('HEAD Object', () => {
        describe('with existing object in bucket', () => {
            beforeEach(async function beforeFn() {
                await setupExistingObject(this.currentTest);
            });

            afterEach(async function afterFn() {
                await cleanupObject(this.currentTest);
            });

            it('should successfully retrieve object', async function testFn() {
                const res = await gcpClient.send(new HeadObjectCommand({
                    Bucket: bucketName,
                    Key: this.test.key,
                }));
                assert.strictEqual(res.ETag, this.test.ETag);
                assert.ok(res.$metadata && res.$metadata.httpStatusCode === 200);
            });
        });

        describe('without existing object in bucket', () => {
            it('should return 404', async () => {
                const badObjectKey = `nonexistingkey-${genUniqID()}`;
                await new Promise(resolve => {
                    gcpClient.headObject({
                        Bucket: bucketName,
                        Key: badObjectKey,
                    }, err => {
                        assert(err);
                        assert.strictEqual(err.$metadata.httpStatusCode, 404);
                        resolve();
                    });
                });
            });
        });
    });

    describe('GET Object', () => {
        describe('with existing object in bucket', () => {
            beforeEach(async function beforeFn() {
                await setupExistingObject(this.currentTest);
            });

            afterEach(async function afterFn() {
                await cleanupObject(this.currentTest);
            });

            it('should successfully retrieve object', async function testFn() {
                const res = await gcpClient.send(new GetObjectCommand({
                    Bucket: bucketName,
                    Key: this.test.key,
                }));
                assert.strictEqual(res.ETag, this.test.ETag);
                assert.strictEqual(res.VersionId, this.test.uploadId);
            });
        });

        describe('without existing object in bucket', () => {
            it('should return 404 and NoSuchKey', done => {
                const badObjectKey = `nonexistingkey-${genUniqID()}`;
                gcpClient.getObject({
                    Bucket: bucketName,
                    Key: badObjectKey,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                    assert.strictEqual(err.name, 'NoSuchKey');
                    return done();
                });
            });
        });
    });

    describe('PUT Object', () => {
        afterEach(async function afterFn() {
            await cleanupObject(this.currentTest);
        });

        describe('with existing object in bucket', () => {
            beforeEach(async function beforeFn() {
                await setupExistingObject(this.currentTest);
            });

            it('should overwrite object', function testFn(done) {
                gcpClient.putObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                }, (err, res) => {
                    assert.notStrictEqual(res.VersionId, this.test.uploadId);
                    return done();
                });
            });
        });

        describe('without existing object in bucket', () => {
            it('should successfully put object', function testFn(done) {
                this.test.key = `somekey-${genUniqID()}`;
                gcpClient.putObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                }, (err, putRes) => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    gcpClient.getObject({
                        Bucket: bucketName,
                        Key: this.test.key,
                    }, (getErr, getRes) => {
                        assert.equal(getErr, null,
                            `Expected success, got error ${getErr}`);
                        assert.strictEqual(getRes.VersionId, putRes.VersionId);
                        return done();
                    });
                });
            });
        });
    });

    describe('DELETE Object', () => {
        const objectKey = `somekey-${genUniqID()}`;
        const badObjectKey = `nonexistingkey-${genUniqID()}`;

        describe('with existing object in bucket', () => {
            beforeEach(async () => {
                await gcpClient.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectKey,
                }));
            });

            it('should successfully delete object', done => {
                async.waterfall([
                    next => gcpClient.deleteObject({
                        Bucket: bucketName,
                        Key: objectKey,
                    }, err => {
                        assert.equal(err, null,
                            `Expected success, got error ${err}`);
                        return next();
                    }),
                    next => {
                        gcpClient.send(new GetObjectCommand({
                            Bucket: bucketName,
                            Key: objectKey,
                        }))
                            .then(() => {
                                assert.fail('Expected NoSuchKey error');
                            })
                            .catch(err => {
                                assert(err);
                                assert.strictEqual(
                                    err.$metadata && err.$metadata.httpStatusCode,
                                    404);
                                assert.strictEqual(err.name, 'NoSuchKey');
                                return next();
                            });
                    },
                ], err => done(err));
            });
        });

        describe('without existing object in bucket', () => {
            it('should return 404 and NoSuchKey', done => {
                gcpClient.deleteObject({
                    Bucket: bucketName,
                    Key: badObjectKey,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                    assert.strictEqual(err.name, 'NoSuchKey');
                    return done();
                });
            });
        });
    });

    describe('COPY Object', () => {
        describe('without existing object in bucket', () => {
            it('should return 404 and \'NoSuchKey\'', done => {
                const missingObject = `nonexistingkey-${genUniqID()}`;
                const someKey = `somekey-${genUniqID()}`;
                gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: someKey,
                    CopySource: `/${bucketName}/${missingObject}`,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                    assert.strictEqual(err.name, 'NoSuchKey');
                    return done();
                });
            });
        });

        describe('with existing object in bucket', () => {
            beforeEach(async function beforeFn() {
                this.currentTest.key = `somekey-${genUniqID()}`;
                this.currentTest.copyKey = `copykey-${genUniqID()}`;
                this.currentTest.initValue = `${genUniqID()}`;
                const res = await gcpClient.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: this.currentTest.copyKey,
                    Metadata: {
                        value: this.currentTest.initValue,
                    },
                }));
                this.currentTest.ETag = res.ETag;
            });

            afterEach(function afterFn(done) {
                async.parallel([
                    next => gcpClient.deleteObject({
                        Bucket: bucketName,
                        Key: this.currentTest.key,
                    }, err => {
                        if (err) {
                            process.stdout.write(`err in deleting object ${err}\n`);
                        }
                        return next(err);
                    }),
                    next => gcpClient.deleteObject({
                        Bucket: bucketName,
                        Key: this.currentTest.copyKey,
                    }, err => {
                        if (err) {
                            process.stdout
                                .write(`err in deleting copy object ${err}\n`);
                        }
                        return next(err);
                    }),
                ], done);
            });

            it('should successfully copy with REPLACE directive',
            function testFn(done) {
                const newValue = `${genUniqID()}`;
                async.waterfall([
                    next => gcpClient.copyObject({
                        Bucket: bucketName,
                        Key: this.test.key,
                        CopySource: `/${bucketName}/${this.test.copyKey}`,
                        MetadataDirective: 'REPLACE',
                        Metadata: {
                            value: newValue,
                        },
                    }, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    }),
                    next => gcpClient.headObject({
                        Bucket: bucketName,
                        Key: this.test.key,
                    }, (err, res) => {
                        if (err) {
                            process.stdout
                                .write(`err in retrieving object ${err}\n`);
                            return next(err);
                        }
                        assert.strictEqual(res.ETag, this.test.ETag);
                        assert.notStrictEqual(res.Metadata.value,
                            this.test.initValue);
                        return next();
                    }),
                ], done);
            });

            it('should successfully copy with COPY directive',
            function testFn(done) {
                async.waterfall([
                    next => gcpClient.copyObject({
                        Bucket: bucketName,
                        Key: this.test.key,
                        CopySource: `/${bucketName}/${this.test.copyKey}`,
                        MetadataDirective: 'COPY',
                    }, err => {
                        assert.equal(err, null,
                            `Expected success, but got error ${err}`);
                        return next();
                    }),
                    next => gcpClient.headObject({
                        Bucket: bucketName,
                        Key: this.test.key,
                    }, (err, res) => {
                        if (err) {
                            process.stdout
                                .write(`err in retrieving object ${err}\n`);
                            return next(err);
                        }
                        assert.strictEqual(res.ETag, this.test.ETag);
                        assert.strictEqual(res.Metadata.value,
                            this.test.initValue);
                        return next();
                    }),
                ], done);
            });
        });
    });
});
