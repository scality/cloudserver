const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genPutTagObj, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const gcpTagPrefix = `x-goog-meta-${gcpTaggingPrefix}`;

describe('GCP: PUT Object Tagging', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    let config;
    let gcpClient;

    beforeAll(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}`);
            }
            return done(err);
        });
    });

    beforeEach(done => {
        testContext.currentTest.key = `somekey-${genUniqID()}`;
        testContext.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: testContext.currentTest.key,
            authCredentials: config.credentials,
        }, (err, res) => {
            if (err) {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            }
            testContext.currentTest.versionId = res.headers['x-goog-generation'];
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
                process.stdout.write(`err in deleting object ${err}`);
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
                process.stdout.write(`err in deleting bucket ${err}`);
            }
            return done(err);
        });
    });

    test('should successfully put object tags', done => {
        async.waterfall([
            next => gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: testContext.test.key,
                VersionId: testContext.test.versionId,
                Tagging: {
                    TagSet: [
                        {
                            Key: testContext.test.specialKey,
                            Value: testContext.test.specialKey,
                        },
                    ],
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
                headers: {
                    'x-goog-generation': testContext.test.versionId,
                },
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in retrieving object ${err}`);
                    return next(err);
                }
                const toCompare =
                    res.headers[`${gcpTagPrefix}${testContext.test.specialKey}`];
                expect(toCompare).toBe(testContext.test.specialKey);
                return next();
            }),
        ], done);
    });

    describe('when tagging parameter is incorrect', () => {
        test('should return 400 and BadRequest if more than ' +
        '10 tags are given', done => {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: testContext.test.key,
                VersionId: testContext.test.versionId,
                Tagging: {
                    TagSet: genPutTagObj(11),
                },
            }, err => {
                expect(err).toBeTruthy();
                expect(err.code).toBe(400);
                expect(err.message).toBe('BadRequest');
                return done();
            });
        });

        test(
            'should return 400 and InvalidTag if given duplicate keys',
            done => {
                return gcpClient.putObjectTagging({
                    Bucket: bucketName,
                    Key: testContext.test.key,
                    VersionId: testContext.test.versionId,
                    Tagging: {
                        TagSet: genPutTagObj(10, true),
                    },
                }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe(400);
                    expect(err.message).toBe('InvalidTag');
                    return done();
                });
            }
        );

        test(
            'should return 400 and InvalidTag if given invalid key',
            done => {
                return gcpClient.putObjectTagging({
                    Bucket: bucketName,
                    Key: testContext.test.key,
                    VersionId: testContext.test.versionId,
                    Tagging: {
                        TagSet: [
                            { Key: Buffer.alloc(129, 'a'), Value: 'bad tag' },
                        ],
                    },
                }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe(400);
                    expect(err.message).toBe('InvalidTag');
                    return done();
                });
            }
        );

        test(
            'should return 400 and InvalidTag if given invalid value',
            done => {
                return gcpClient.putObjectTagging({
                    Bucket: bucketName,
                    Key: testContext.test.key,
                    VersionId: testContext.test.versionId,
                    Tagging: {
                        TagSet: [
                            { Key: 'badtag', Value: Buffer.alloc(257, 'a') },
                        ],
                    },
                }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe(400);
                    expect(err.message).toBe('InvalidTag');
                    return done();
                });
            }
        );
    });
});
