const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genPutTagObj, genUniqID, gcpRetry } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `cldsrvci-puttagging-${genUniqID()}`;

describe('GCP: PUT Object Tagging', () => {
    let config;
    let gcpClient;

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        await gcpRetry(
            gcpClient,
            new CreateBucketCommand({ Bucket: bucketName }),
        );
    });

    beforeEach(async function beforeFn() {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const cmd = new PutObjectCommand({
            Bucket: bucketName,
            Key: this.currentTest.key,
        });
        const res = await gcpClient.send(cmd);
        this.currentTest.versionId = res.VersionId;
    });

    afterEach(function afterFn(done) {
        gcpClient.deleteObject({
            Bucket: bucketName,
            Key: this.currentTest.key,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting object ${err}`);
            }
            return done(err);
        });
    });

    after(async () => {
        await gcpRetry(
            gcpClient,
            new DeleteBucketCommand({ Bucket: bucketName }),
        );
    });

    it('should successfully put object tags', function testFn(done) {
        async.waterfall([
            next => gcpClient.putObjectTagging({
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
            }, err => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                return next();
            }),
            next => gcpClient.headObject({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in retrieving object ${err}`);
                    return next(err);
                }
                const metaKey = `${gcpTaggingPrefix}${this.test.specialKey}`;
                const toCompare = res.Metadata[metaKey];
                assert.strictEqual(toCompare, this.test.specialKey);
                return next();
            }),
        ], done);
    });

    describe('when tagging parameter is incorrect', () => {
        it('should return 400 and BadRequest if more than ' +
        '10 tags are given', function testFun(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: genPutTagObj(11),
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'BadRequest');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given duplicate keys',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: genPutTagObj(10, true),
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given invalid key',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: [
                        { Key: Buffer.alloc(129, 'a'), Value: 'bad tag' },
                    ],
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given invalid value',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: [
                        { Key: 'badtag', Value: Buffer.alloc(257, 'a') },
                    ],
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });
    });
});
