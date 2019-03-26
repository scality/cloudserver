const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: COPY Object', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    this.timeout(180000);
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
                process.stdout.write(`err in creating bucket ${err}\n`);
            }
            return done(err);
        });
    });

    describe('without existing object in bucket', () => {
        test('should return 404 and \'NoSuchKey\'', done => {
            const missingObject = `nonexistingkey-${genUniqID()}`;
            const someKey = `somekey-${genUniqID()}`;
            gcpClient.copyObject({
                Bucket: bucketName,
                Key: someKey,
                CopySource: `/${bucketName}/${missingObject}`,
            }, err => {
                expect(err).toBeTruthy();
                expect(err.statusCode).toBe(404);
                expect(err.code).toBe('NoSuchKey');
                return done();
            });
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(done => {
            testContext.currentTest.key = `somekey-${genUniqID()}`;
            testContext.currentTest.copyKey = `copykey-${genUniqID()}`;
            testContext.currentTest.initValue = `${genUniqID()}`;
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey: testContext.currentTest.copyKey,
                headers: {
                    'x-goog-meta-value': testContext.currentTest.initValue,
                },
                authCredentials: config.credentials,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                }
                testContext.currentTest.contentHash = res.headers['x-goog-hash'];
                return done(err);
            });
        });

        afterEach(done => {
            async.parallel([
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
                    objectKey: testContext.currentTest.key,
                    authCredentials: config.credentials,
                }, err => {
                    if (err) {
                        process.stdout.write(`err in deleting object ${err}\n`);
                    }
                    return next(err);
                }),
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
                    objectKey: testContext.currentTest.copyKey,
                    authCredentials: config.credentials,
                }, err => {
                    if (err) {
                        process.stdout
                            .write(`err in deleting copy object ${err}\n`);
                    }
                    return next(err);
                }),
            ], done);
        });

        test('should successfully copy with REPLACE directive', done => {
            const newValue = `${genUniqID()}`;
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: testContext.test.key,
                    CopySource: `/${bucketName}/${testContext.test.copyKey}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        value: newValue,
                    },
                }, err => {
                    expect(err).toEqual(null);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: testContext.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    expect(testContext.test.contentHash).toBe(res.headers['x-goog-hash']);
                    expect(res.headers['x-goog-meta-value']).not.toBe(testContext.test.initValue);
                    return next();
                }),
            ], done);
        });

        test('should successfully copy with COPY directive', done => {
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: testContext.test.key,
                    CopySource: `/${bucketName}/${testContext.test.copyKey}`,
                    MetadataDirective: 'COPY',
                }, err => {
                    expect(err).toEqual(null);
                    return next();
                }),
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: testContext.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    expect(testContext.test.contentHash).toBe(res.headers['x-goog-hash']);
                    expect(res.headers['x-goog-meta-value']).toBe(testContext.test.initValue);
                    return next();
                }),
            ], done);
        });
    });
});
