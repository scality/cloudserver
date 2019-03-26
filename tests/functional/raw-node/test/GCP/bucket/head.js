const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';

describe('GCP: HEAD Bucket', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    describe('without existing bucket', () => {
        beforeEach(done => {
            testContext.currentTest.bucketName = `somebucket-${genUniqID()}`;
            return done();
        });

        test('should return 404', done => {
            gcpClient.headBucket({
                Bucket: testContext.test.bucketName,
            }, err => {
                expect(err).toBeTruthy();
                expect(err.statusCode).toBe(404);
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        beforeEach(done => {
            testContext.currentTest.bucketName = `somebucket-${genUniqID()}`;
            process.stdout
                .write(`Creating test bucket ${testContext.currentTest.bucketName}\n`);
            gcpRequestRetry({
                method: 'PUT',
                bucket: testContext.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, (err, res) => {
                if (err) {
                    return done(err);
                }
                testContext.currentTest.bucketObj = {
                    MetaVersionId: res.headers['x-goog-metageneration'],
                };
                return done();
            });
        });

        afterEach(done => {
            gcpRequestRetry({
                method: 'DELETE',
                bucket: testContext.currentTest.bucketName,
                authCredentials: config.credentials,
            }, 0, err => {
                if (err) {
                    process.stdout
                        .write(`err deleting bucket: ${err.code}\n`);
                }
                return done(err);
            });
        });

        test('should get bucket information', done => {
            gcpClient.headBucket({
                Bucket: testContext.test.bucketName,
            }, (err, res) => {
                expect(err).toEqual(null);
                assert.deepStrictEqual(testContext.test.bucketObj, res);
                return done();
            });
        });
    });
});
