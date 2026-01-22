const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: GET Object', function testSuite() {
    this.timeout(30000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(done => {
        const cmd = new CreateBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in creating bucket ${err}\n`);
                return done(err);
            });
    });

    after(done => {
        const cmd = new DeleteBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in deleting bucket ${err}\n`);
                return done(err);
            });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${genUniqID()}`;
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.key,
            });
            gcpClient.send(cmd)
                .then(res => {
                    this.currentTest.uploadId = res.VersionId;
                    this.currentTest.ETag = res.ETag;
                    return done();
                })
                .catch(err => {
                    process.stdout.write(`err in creating object ${err}\n`);
                    return done(err);
                });
        });

        afterEach(function afterFn(done) {
            const cmd = new DeleteObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.key,
            });
            gcpClient.send(cmd)
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`err in deleting object ${err}\n`);
                    return done(err);
                });
        });

        it('should successfully retrieve object', function testFn(done) {
            const cmd = new GetObjectCommand({
                Bucket: bucketName,
                Key: this.test.key,
            });
            gcpClient.send(cmd)
                .then(res => {
                    assert.strictEqual(res.ETag, this.test.ETag);
                    assert.ok(res.$metadata && typeof res.$metadata.httpStatusCode === 'number');
                    return done();
                })
                .catch(err => {
                    process.stdout.write(`err in getting object ${err}\n`);
                    return done(err);
                });
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
