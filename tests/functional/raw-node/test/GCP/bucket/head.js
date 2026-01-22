const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';

describe('GCP: HEAD Bucket', () => {
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    describe('without existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${genUniqID()}`;
            return done();
        });

        it('should return 404', function testFn(done) {
            gcpClient.headBucket({
                Bucket: this.test.bucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.bucketName = `somebucket-${genUniqID()}`;
            process.stdout
                .write(`Creating test bucket ${this.currentTest.bucketName}\n`);
            const cmd = new CreateBucketCommand({
                Bucket: this.currentTest.bucketName,
            });
            gcpClient.send(cmd)
                .then(() => done())
                .catch(err => done(err));
        });

        afterEach(function afterFn(done) {
            const cmd = new DeleteBucketCommand({
                Bucket: this.currentTest.bucketName,
            });
            gcpClient.send(cmd)
                .then(() => done())
                .catch(err => {
                    process.stdout
                        .write(`err deleting bucket: ${err.code}\n`);
                    return done(err);
                });
        });

        it('should get bucket information', function testFn(done) {
            gcpClient.headBucket({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                assert.equal(err, null, `Expected success, but got ${err}`);
                const { $metadata, ...data } = res;
                assert.strictEqual($metadata.httpStatusCode, 200);
                // Ensure MetaVersionId is present and non-empty
                assert.ok(
                    typeof data.MetaVersionId === 'string'
                    && data.MetaVersionId.length > 0
                );
                return done();
            });
        });
    });
});
