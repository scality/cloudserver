const assert = require('assert');
const async = require('async');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const {
    describeSkipIfNotMultipleOrCeph,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    azureLocation,
    azureLocationMismatch,
} = require('../utils');

const keyObject = 'deleteazure';
const azureContainerName = getAzureContainerName(azureLocation);
const keys = getAzureKeys();
const azureClient = getAzureClient();

const normalBody = Buffer.from('I am a body', 'utf8');
const azureTimeout = 20000;

const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

describeSkipIfNotMultipleOrCeph('Multiple backend delete object from Azure',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeAll(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: azureContainerName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterAll(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });
        keys.forEach(key => {
            const keyName = uniqName(keyObject);
            describe(`${key.describe} size`, () => {
                beforeAll(done => {
                    s3.putObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                        Body: key.body,
                        Metadata: {
                            'scal-location-constraint': azureLocation,
                        },
                    }, done);
                });

                test(`should delete an ${key.describe} object from Azure`, done => {
                    s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: keyName,
                    }, err => {
                        expect(err).toEqual(null);
                        setTimeout(() =>
                        azureClient.getBlobProperties(azureContainerName,
                        keyName, err => {
                            expect(err.statusCode).toBe(404);
                            expect(err.code).toBe('NotFound');
                            return done();
                        }), azureTimeout);
                    });
                });
            });
        });

        describe('delete from Azure location with bucketMatch set to false',
        () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocationMismatch,
                    },
                }, done);
            });

            test('should delete object', done => {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: testContext.test.azureObject,
                }, err => {
                    expect(err).toEqual(null);
                    setTimeout(() =>
                    azureClient.getBlobProperties(azureContainerName,
                    `${azureContainerName}/${testContext.test.azureObject}`,
                    err => {
                        expect(err.statusCode).toBe(404);
                        expect(err.code).toBe('NotFound');
                        return done();
                    }), azureTimeout);
                });
            });
        });

        describe('returning no error', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, err => {
                    expect(err).toEqual(null);
                    azureClient.deleteBlob(azureContainerName,
                    testContext.currentTest.azureObject, err => {
                        expect(err).toEqual(null);
                        done(err);
                    });
                });
            });

            test('should return no error on deleting an object deleted ' +
            'from Azure', done => {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: testContext.test.azureObject,
                }, err => {
                    expect(err).toEqual(null);
                    done();
                });
            });
        });

        describe('Versioning:: ', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.azureObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                }, done);
            });

            test('should not delete object when deleting a non-existing ' +
            'version from Azure', done => {
                async.waterfall([
                    next => s3.deleteObject({
                        Bucket: azureContainerName,
                        Key: testContext.test.azureObject,
                        VersionId: nonExistingId,
                    }, err => next(err)),
                    next => s3.getObject({
                        Bucket: azureContainerName,
                        Key: testContext.test.azureObject,
                    }, (err, res) => {
                        expect(err).toEqual(null);
                        assert.deepStrictEqual(res.Body, normalBody);
                        return next(err);
                    }),
                    next => azureClient.getBlobToText(azureContainerName,
                    testContext.test.azureObject, (err, res) => {
                        expect(err).toEqual(null);
                        assert.deepStrictEqual(Buffer.from(res, 'utf8'),
                        normalBody);
                        return next();
                    }),
                ], done);
            });
        });

        describe('with ongoing MPU: ', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done => {
                testContext.currentTest.key = uniqName(keyObject);
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.key,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.putObject(params, err => {
                    expect(err).toEqual(null);
                    const params = {
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    };
                    s3.createMultipartUpload(params, (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadId = res.UploadId;
                        setTimeout(() => done(), azureTimeout);
                    });
                });
            });

            afterEach(done => {
                s3.abortMultipartUpload({
                    Bucket: azureContainerName,
                    Key: testContext.currentTest.key,
                    UploadId: testContext.currentTest.uploadId,
                }, err => {
                    expect(err).toEqual(null);
                    setTimeout(() => done(), azureTimeout);
                });
            });

            test('should return InternalError', done => {
                s3.deleteObject({
                    Bucket: azureContainerName,
                    Key: testContext.test.key,
                }, err => {
                    expect(err.code).toBe('MPUinProgress');
                    done();
                });
            });
        });
    });
});
