const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const {
    genPutTagObj,
    genGetTagObj,
    genDelTagObj,
    genUniqID,
    genBucketName,
    gcpRetry,
} = require('../../../utils/gcpUtils');
const { getRealAwsConfig } = require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');
const { CreateBucketCommand, DeleteBucketCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';

describe('GCP: Object Tagging', function testSuite() {
    this.timeout(120000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);
    const bucketName = genBucketName('tagging');

    before(async () => {
        await gcpRetry(gcpClient, new CreateBucketCommand({ Bucket: bucketName }));
    });

    after(async () => {
        await gcpRetry(gcpClient, new DeleteBucketCommand({ Bucket: bucketName }));
    });

    afterEach(function afterFn(done) {
        gcpClient.deleteObject(
            {
                Bucket: bucketName,
                Key: this.currentTest.key,
            },
            err => {
                if (err) {
                    process.stdout.write(`err in deleting object ${err}`);
                }
                return done(err);
            },
        );
    });

    describe('PUT Object Tagging', () => {
        beforeEach(async function beforeFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
            const res = await gcpClient.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: this.currentTest.key,
                }),
            );
            this.currentTest.versionId = res.VersionId;
        });

        it('should successfully put object tags', function testFn(done) {
            async.waterfall(
                [
                    next =>
                        gcpClient.putObjectTagging(
                            {
                                Bucket: bucketName,
                                Key: this.test.key,
                                VersionId: this.test.versionId,
                                Tagging: {
                                    TagSet: [
                                        {
                                            Key: this.test.specialKey,
                                            Value: this.test.specialKey,
                                        },
                                    ],
                                },
                            },
                            err => {
                                assert.equal(err, null, `Expected success, got error ${err}`);
                                return next();
                            },
                        ),
                    next =>
                        gcpClient.headObject(
                            {
                                Bucket: bucketName,
                                Key: this.test.key,
                                VersionId: this.test.versionId,
                            },
                            (err, res) => {
                                if (err) {
                                    process.stdout.write(`err in retrieving object ${err}`);
                                    return next(err);
                                }
                                const metaKey = `${gcpTaggingPrefix}${this.test.specialKey}`;
                                const toCompare = res.Metadata[metaKey];
                                assert.strictEqual(toCompare, this.test.specialKey);
                                return next();
                            },
                        ),
                ],
                done,
            );
        });

        describe('when tagging parameter is incorrect', () => {
            it('should return 400 and BadRequest if more than ' + '10 tags are given', function testFun(done) {
                return gcpClient.putObjectTagging(
                    {
                        Bucket: bucketName,
                        Key: this.test.key,
                        VersionId: this.test.versionId,
                        Tagging: {
                            TagSet: genPutTagObj(11),
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.code, 400);
                        assert.strictEqual(err.message, 'BadRequest');
                        return done();
                    },
                );
            });

            it('should return 400 and InvalidTag if given duplicate keys', function testFn(done) {
                return gcpClient.putObjectTagging(
                    {
                        Bucket: bucketName,
                        Key: this.test.key,
                        VersionId: this.test.versionId,
                        Tagging: {
                            TagSet: genPutTagObj(10, true),
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.code, 400);
                        assert.strictEqual(err.message, 'InvalidTag');
                        return done();
                    },
                );
            });

            it('should return 400 and InvalidTag if given invalid key', function testFn(done) {
                return gcpClient.putObjectTagging(
                    {
                        Bucket: bucketName,
                        Key: this.test.key,
                        VersionId: this.test.versionId,
                        Tagging: {
                            TagSet: [{ Key: Buffer.alloc(129, 'a'), Value: 'bad tag' }],
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.code, 400);
                        assert.strictEqual(err.message, 'InvalidTag');
                        return done();
                    },
                );
            });

            it('should return 400 and InvalidTag if given invalid value', function testFn(done) {
                return gcpClient.putObjectTagging(
                    {
                        Bucket: bucketName,
                        Key: this.test.key,
                        VersionId: this.test.versionId,
                        Tagging: {
                            TagSet: [{ Key: 'badtag', Value: Buffer.alloc(257, 'a') }],
                        },
                    },
                    err => {
                        assert(err);
                        assert.strictEqual(err.code, 400);
                        assert.strictEqual(err.message, 'InvalidTag');
                        return done();
                    },
                );
            });
        });
    });

    describe('GET Object Tagging', () => {
        const tagSize = 10;

        beforeEach(async function beforeFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
            const { expectedTagObj } = genGetTagObj(tagSize, `x-goog-meta-${gcpTaggingPrefix}`);
            this.currentTest.tagObj = expectedTagObj;

            const res = await gcpClient.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: this.currentTest.key,
                }),
            );
            this.currentTest.versionId = res.VersionId;

            await new Promise((resolve, reject) => {
                gcpClient.putObjectTagging(
                    {
                        Bucket: bucketName,
                        Key: this.currentTest.key,
                        VersionId: this.currentTest.versionId,
                        Tagging: {
                            TagSet: this.currentTest.tagObj,
                        },
                    },
                    err => {
                        if (err) {
                            process.stdout.write(`err in setting object tags ${err}`);
                            reject(err);
                            return;
                        }
                        resolve();
                    },
                );
            });
        });

        it('should successfully get object tags', function testFn(done) {
            gcpClient.getObjectTagging(
                {
                    Bucket: bucketName,
                    Key: this.test.key,
                    VersionId: this.test.versionId,
                },
                (err, res) => {
                    assert.equal(err, null, `Expected success, got error ${err}`);
                    assert.deepStrictEqual(res.TagSet, this.test.tagObj);
                    return done();
                },
            );
        });
    });

    describe('DELETE Object Tagging', () => {
        function assertObjectMetaTag(params, callback) {
            return gcpClient.headObject(
                {
                    Bucket: params.bucket,
                    Key: params.key,
                    VersionId: params.versionId,
                },
                (err, res) => {
                    if (err) {
                        process.stdout.write(`err in retrieving object ${err}`);
                        return callback(err);
                    }
                    const resMeta = Object.assign({}, res.Metadata || {});
                    const tagRes = {};
                    const metaRes = {};
                    Object.keys(resMeta).forEach(key => {
                        if (key.startsWith(gcpTaggingPrefix)) {
                            tagRes[key] = resMeta[key];
                        } else {
                            metaRes[key] = resMeta[key];
                        }
                    });
                    assert.deepStrictEqual(params.tag, tagRes);
                    assert.deepStrictEqual(params.meta, metaRes);
                    return callback();
                },
            );
        }

        beforeEach(async function beforeFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
            const { expectedTagObj, expectedMetaObj } = genDelTagObj(10, `x-goog-meta-${gcpTaggingPrefix}`);

            const expectedTagMeta = {};
            Object.keys(expectedTagObj).forEach(header => {
                const key = header.replace('x-goog-meta-', '');
                expectedTagMeta[key] = expectedTagObj[header];
            });

            const expectedMetaMeta = {};
            Object.keys(expectedMetaObj).forEach(header => {
                const key = header.replace('x-goog-meta-', '');
                expectedMetaMeta[key] = expectedMetaObj[header];
            });

            this.currentTest.expectedTagObj = expectedTagMeta;
            this.currentTest.expectedMetaObj = expectedMetaMeta;

            const res = await gcpClient.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: this.currentTest.key,
                    Metadata: Object.assign({}, expectedTagMeta, expectedMetaMeta),
                }),
            );
            this.currentTest.versionId = res.VersionId;
        });

        it('should successfully delete object tags', function testFn(done) {
            async.waterfall(
                [
                    next =>
                        assertObjectMetaTag(
                            {
                                bucket: bucketName,
                                key: this.test.key,
                                versionId: this.test.versionId,
                                meta: this.test.expectedMetaObj,
                                tag: this.test.expectedTagObj,
                            },
                            next,
                        ),
                    next =>
                        gcpClient.deleteObjectTagging(
                            {
                                Bucket: bucketName,
                                Key: this.test.key,
                                VersionId: this.test.versionId,
                            },
                            err => {
                                assert.equal(err, null, `Expected success, got error ${err}`);
                                return next();
                            },
                        ),
                    next =>
                        assertObjectMetaTag(
                            {
                                bucket: bucketName,
                                key: this.test.key,
                                versionId: this.test.versionId,
                                meta: this.test.expectedMetaObj,
                                tag: {},
                            },
                            next,
                        ),
                ],
                done,
            );
        });
    });
});
