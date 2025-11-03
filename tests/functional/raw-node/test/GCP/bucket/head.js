const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

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
                console.log('[HEAD Bucket Test] Callback - err:', err ? err.code : 'success');
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
            gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, (err, res) => {
                if (err) {
                    return done(err);
                }
                console.log('[HEAD Bucket Test] Bucket created - res:', res);
                this.currentTest.bucketObj = {
                    MetaVersionId: res.headers['x-goog-metageneration'],
                };
                return done();
            });
        });

        afterEach(function afterFn(done) {
            gcpRequestRetry({
                method: 'DELETE',
                bucket: this.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout
                        .write(`err deleting bucket: ${err.code}\n`);
                }
                return done(err);
            });
        });

        it('should get bucket information', function testFn(done) {
            console.log('[HEAD Bucket Test] Getting info for bucket:', this.test.bucketName);
            gcpClient.headBucket({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                console.log('[HEAD Bucket Test] Callback - err:', err);
                console.log('[HEAD Bucket Test] Callback - res:', res);
                console.log('[HEAD Bucket Test] Callback - data:', this.test.bucketObj);
                assert.equal(err, null, `Expected success, but got ${err}`);
                const { $metadata, ...data } = res;
                console.log('[HEAD Bucket Test] Callback - res:', res);
                console.log('[HEAD Bucket Test] Callback - $metadata:', $metadata);
                console.log('[HEAD Bucket Test] Callback - data:', this.test.bucketObj);
                assert.strictEqual($metadata.httpStatusCode, 200);
                assert.deepStrictEqual(this.test.bucketObj, data);
                return done();
            });
        });
    });
});
