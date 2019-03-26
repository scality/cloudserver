const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: HEAD Object', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    this.timeout(30000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    beforeAll(done => {
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

    afterAll(done => {
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
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey: testContext.currentTest.key,
                authCredentials: config.credentials,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                    return done(err);
                }
                testContext.currentTest.uploadId =
                    res.headers['x-goog-generation'];
                testContext.currentTest.ETag = res.headers.etag;
                return done();
            });
        });

        afterEach(done => {
            makeGcpRequest({
                method: 'DELETE',
                bucket: bucketName,
                objectKey: testContext.currentTest.key,
                authCredentials: config.credentials,
            }, err => {
                if (err) {
                    process.stdout.write(`err in deleting object ${err}\n`);
                }
                return done(err);
            });
        });

        test('should successfully retrieve object', done => {
            gcpClient.headObject({
                Bucket: bucketName,
                Key: testContext.test.key,
            }, (err, res) => {
                expect(err).toEqual(null);
                expect(res.ETag).toBe(testContext.test.ETag);
                expect(res.VersionId).toBe(testContext.test.uploadId);
                return done();
            });
        });
    });

    describe('without existing object in bucket', () => {
        test('should return 404', done => {
            const badObjectkey = `nonexistingkey-${genUniqID()}`;
            gcpClient.headObject({
                Bucket: bucketName,
                Key: badObjectkey,
            }, err => {
                expect(err).toBeTruthy();
                expect(err.statusCode).toBe(404);
                return done();
            });
        });
    });
});
