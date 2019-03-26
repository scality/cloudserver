const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultipleOrCeph,
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    convertMD5,
    fileLocation,
    azureLocation,
    azureLocationMismatch,
} = require('../utils');

const keyObject = 'putazure';
const azureClient = getAzureClient();
const azureContainerName = getAzureContainerName(azureLocation);
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

const normalBody = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const keys = getAzureKeys();
/* eslint-disable camelcase */
const azureMetadata = {
    scal_location_constraint: azureLocation,
};
/* eslint-enable camelcase */

const azureTimeout = 20000;
let bucketUtil;
let s3;

function azureGetCheck(objectKey, azureMD5, azureMetadata, cb) {
    azureClient.getBlobProperties(azureContainerName, objectKey,
    (err, res) => {
        expect(err).toBe(null);
        const resMD5 = convertMD5(res.contentSettings.contentMD5);
        expect(resMD5).toBe(azureMD5);
        assert.deepStrictEqual(res.metadata, azureMetadata);
        return cb();
    });
}

describeSkipIfNotMultipleOrCeph('MultipleBackend put object to AZURE', function
describeF() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(() => {
            this.currentTest.keyName = uniqName(keyObject);
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(azureContainerName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(azureContainerName);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });
        describe('with bucket location header', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(done =>
                s3.createBucket({ Bucket: azureContainerName,
                    CreateBucketConfiguration: {
                        LocationConstraint: azureLocation,
                    },
                }, done));

            test('should return a NotImplemented error if try to put ' +
            'versioning to bucket with Azure location', done => {
                const params = {
                    Bucket: azureContainerName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                };
                s3.putBucketVersioning(params, err => {
                    expect(err.code).toBe('NotImplemented');
                    done();
                });
            });

            test('should put an object to Azure, with no object location ' +
            'header, based on bucket location', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.keyName,
                    Body: normalBody,
                };
                async.waterfall([
                    next => s3.putObject(params, err => setTimeout(() =>
                      next(err), azureTimeout)),
                    next => azureGetCheck(testContext.test.keyName, normalMD5, {},
                      next),
                ], done);
            });
        });

        describe('with no bucket location header', () => {
            let testContext;

            beforeEach(() => {
                testContext = {};
            });

            beforeEach(() =>
              s3.createBucketAsync({ Bucket: azureContainerName })
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                }));

            keys.forEach(key => {
                test(`should put a ${key.describe} object to Azure`, done => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: testContext.test.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                        Body: key.body,
                    };
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        setTimeout(() =>
                            azureGetCheck(testContext.test.keyName,
                              key.MD5, azureMetadata,
                            () => done()), azureTimeout);
                    });
                });
            });

            test(
                'should put a object to Azure location with bucketMatch=false',
                done => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: testContext.test.keyName,
                        Metadata: { 'scal-location-constraint':
                        azureLocationMismatch },
                        Body: normalBody,
                    };
                    const azureMetadataMismatch = {
                        /* eslint-disable camelcase */
                        scal_location_constraint: azureLocationMismatch,
                        /* eslint-enable camelcase */
                    };
                    s3.putObject(params, err => {
                        expect(err).toEqual(null);
                        setTimeout(() =>
                            azureGetCheck(
                              `${azureContainerName}/${testContext.test.keyName}`,
                              normalMD5, azureMetadataMismatch,
                            () => done()), azureTimeout);
                    });
                }
            );

            test('should return error ServiceUnavailable putting an invalid ' +
            'key name to Azure', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: '.',
                    Metadata: { 'scal-location-constraint': azureLocation },
                    Body: normalBody,
                };
                s3.putObject(params, err => {
                    expect(err.code).toBe('ServiceUnavailable');
                    done();
                });
            });

            test('should return error NotImplemented putting a ' +
            'version to Azure', done => {
                s3.putBucketVersioning({
                    Bucket: azureContainerName,
                    VersioningConfiguration: versioningEnabled,
                }, err => {
                    expect(err).toEqual(null);
                    const params = { Bucket: azureContainerName,
                        Key: testContext.test.keyName,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        azureLocation } };
                    s3.putObject(params, err => {
                        expect(err.code).toBe('NotImplemented');
                        done();
                    });
                });
            });

            test('should put two objects to Azure with same ' +
            'key, and newest object should be returned', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.keyName,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Body = normalBody;
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
                    },
                    next => {
                        setTimeout(() => {
                            azureGetCheck(testContext.test.keyName, normalMD5,
                              azureMetadata, next);
                        }, azureTimeout);
                    },
                ], done);
            });

            test('should put objects with same key to Azure ' +
            'then file, and object should only be present in file', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: testContext.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': azureLocation } };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = { 'scal-location-constraint':
                        fileLocation };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
                    },
                    next => s3.getObject({
                        Bucket: azureContainerName,
                        Key: testContext.test.keyName,
                    }, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.Metadata['scal-location-constraint']).toBe(fileLocation);
                        next();
                    }),
                    next => azureClient.getBlobProperties(azureContainerName,
                    testContext.test.keyName, err => {
                        expect(err.code).toBe('NotFound');
                        next();
                    }),
                ], done);
            });

            test('should put objects with same key to file ' +
            'then Azure, and object should only be present on Azure', done => {
                const params = { Bucket: azureContainerName, Key:
                    testContext.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = {
                            'scal-location-constraint': azureLocation,
                        };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
                    },
                    next => azureGetCheck(testContext.test.keyName, normalMD5,
                      azureMetadata, next),
                ], done);
            });

            describe('with ongoing MPU with same key name', () => {
                beforeEach(done => {
                    s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, (err, res) => {
                        expect(err).toEqual(null);
                        testContext.currentTest.uploadId = res.UploadId;
                        done();
                    });
                });

                afterEach(done => {
                    s3.abortMultipartUpload({
                        Bucket: azureContainerName,
                        Key: testContext.currentTest.keyName,
                        UploadId: testContext.currentTest.uploadId,
                    }, err => {
                        expect(err).toEqual(null);
                        done();
                    });
                });

                test('should return ServiceUnavailable', done => {
                    s3.putObject({
                        Bucket: azureContainerName,
                        Key: testContext.test.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => {
                        expect(err.code).toBe('ServiceUnavailable');
                        done();
                    });
                });
            });
        });
    });
});
