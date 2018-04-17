const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: GET Object', function testSuite() {
    this.timeout(30000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(done => {
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
            }
            return done(err);
        });
    });

    after(done => {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            }
            return done(err);
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${genUniqID()}`;
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey: this.currentTest.key,
                authCredentials: config.credentials,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                    return done(err);
                }
                this.currentTest.uploadId =
                    res.headers['x-goog-generation'];
                this.currentTest.ETag = res.headers.etag;
                return done();
            });
        });

        afterEach(function afterFn(done) {
            makeGcpRequest({
                method: 'DELETE',
                bucket: bucketName,
                objectKey: this.currentTest.key,
                authCredentials: config.credentials,
            }, err => {
                if (err) {
                    process.stdout.write(`err in deleting object ${err}\n`);
                }
                return done(err);
            });
        });

        it('should successfully retrieve object', function testFn(done) {
            gcpClient.getObject({
                Bucket: bucketName,
                Key: this.test.key,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                assert.strictEqual(res.ETag, this.test.ETag);
                assert.strictEqual(res.VersionId, this.test.uploadId);
                return done();
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
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            });
        });
    });
});
