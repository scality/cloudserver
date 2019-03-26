const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: PUT Object', () => {
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

    describe('with existing object in bucket', () => {
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            gcpRequestRetry({
                method: 'PUT',
                bucket: bucketName,
                objectKey: testContext.currentTest.key,
                authCredentials: config.credentials,
            }, 0, (err, res) => {
                if (err) {
                    process.stdout.write(`err in putting object ${err}\n`);
                    return done(err);
                }
                testContext.currentTest.uploadId =
                    res.headers['x-goog-generation'];
                return done();
            });
        });

        test('should overwrite object', done => {
            gcpClient.putObject({
                Bucket: bucketName,
                Key: testContext.test.key,
            }, (err, res) => {
                expect(res.VersionId).not.toBe(testContext.test.uploadId);
                return done();
            });
        });
    });

    describe('without existing object in bucket', () => {
        test('should successfully put object', done => {
            testContext.test.key = `somekey-${genUniqID()}`;
            gcpClient.putObject({
                Bucket: bucketName,
                Key: testContext.test.key,
            }, (err, putRes) => {
                expect(err).toEqual(null);
                makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey: testContext.test.key,
                    authCredentials: config.credentials,
                }, (err, getRes) => {
                    if (err) {
                        process.stdout.write(`err in getting bucket ${err}\n`);
                        return done(err);
                    }
                    expect(getRes.headers['x-goog-generation']).toBe(putRes.VersionId);
                    return done();
                });
            });
        });
    });
});
