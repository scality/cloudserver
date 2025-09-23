const assert = require('assert');
const async = require('async');
const { CreateBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand,
    PutBucketVersioningCommand } = require('@aws-sdk/client-s3');
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
    azureClient.getContainerClient(azureContainerName).getBlobClient(objectKey).getProperties()
        .then(res => {
            const resMD5 = convertMD5(res.contentSettings.contentMD5);
            assert.strictEqual(resMD5, azureMD5);
            assert.deepStrictEqual(res.metadata, azureMetadata);
            return cb();
        })
        .catch(err => cb(err));
}

describeSkipIfNotMultipleOrCeph('MultipleBackend put object to AZURE', function
describeF() {
    this.timeout(250000);
    withV4(sigCfg => {
        beforeEach(function beforeEachF() {
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
                    beforeEach(done => {
            s3.send(new CreateBucketCommand({ 
                Bucket: azureContainerName,
                CreateBucketConfiguration: {
                    LocationConstraint: azureLocation,
                },
            }))
                .then(() => done())
                .catch(done);
        });

            it('should return a NotImplemented error if try to put ' +
            'versioning to bucket with Azure location', done => {
                const params = {
                    Bucket: azureContainerName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                };
                s3.send(new PutBucketVersioningCommand(params))
                    .then(() => {
                        done(new Error('Expected NotImplemented error'));
                    })
                    .catch(err => {
                        assert.strictEqual(err.name, 'NotImplemented');
                        done();
                    });
            });

            it('should put an object to Azure, with no object location ' +
            'header, based on bucket location', function it(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.keyName,
                    Body: normalBody,
                };
                async.waterfall([
                    next => {
                        s3.send(new PutObjectCommand(params))
                            .then(() => setTimeout(() => next(), azureTimeout))
                            .catch(next);
                    },
                    next => azureGetCheck(this.test.keyName, normalMD5, {},
                      next),
                ], done);
            });
        });

        describe('with no bucket location header', () => {
            beforeEach(() =>
              s3.send(new CreateBucketCommand({ Bucket: azureContainerName }))
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                }));

            keys.forEach(key => {
                it(`should put a ${key.describe} object to Azure`,
                function itF(done) {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.test.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                        Body: key.body,
                    };
                    s3.send(new PutObjectCommand(params))
                        .then(() => {
                            setTimeout(() =>
                                azureGetCheck(this.test.keyName,
                                  key.MD5, azureMetadata,
                                () => done()), azureTimeout);
                        })
                        .catch(done);
                });
            });

            it('should put a object to Azure location with bucketMatch=false',
            function itF(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.keyName,
                    Metadata: { 'scal-location-constraint':
                    azureLocationMismatch },
                    Body: normalBody,
                };
                const azureMetadataMismatch = {
                    /* eslint-disable camelcase */
                    scal_location_constraint: azureLocationMismatch,
                    /* eslint-enable camelcase */
                };
                s3.send(new PutObjectCommand(params))
                    .then(() => {
                        setTimeout(() =>
                            azureGetCheck(
                              `${azureContainerName}/${this.test.keyName}`,
                              normalMD5, azureMetadataMismatch,
                            () => done()), azureTimeout);
                    })
                    .catch(done);
            });

            it('should return error ServiceUnavailable putting an invalid ' +
            'key name to Azure', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: '.',
                    Metadata: { 'scal-location-constraint': azureLocation },
                    Body: normalBody,
                };
                s3.send(new PutObjectCommand(params))
                    .then(() => {
                        done(new Error('Expected ServiceUnavailable error'));
                    })
                    .catch(err => {
                        assert.strictEqual(err.name, 'ServiceUnavailable');
                        done();
                    });
            });

            it('should return error NotImplemented putting a ' +
            'version to Azure', function itF(done) {
                s3.send(new PutBucketVersioningCommand({
                    Bucket: azureContainerName,
                    VersioningConfiguration: versioningEnabled,
                }))
                    .then(() => {
                        const params = { Bucket: azureContainerName,
                            Key: this.test.keyName,
                            Body: normalBody,
                            Metadata: { 'scal-location-constraint':
                            azureLocation } };
                        return s3.send(new PutObjectCommand(params));
                    })
                    .then(() => {
                        done(new Error('Expected NotImplemented error'));
                    })
                    .catch(err => {
                        assert.strictEqual(err.name, 'NotImplemented');
                        done();
                    });
            });

            it('should put two objects to Azure with same ' +
            'key, and newest object should be returned', function itF(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.keyName,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                async.waterfall([
                    next => {
                        s3.send(new PutObjectCommand(params))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        params.Body = normalBody;
                        s3.send(new PutObjectCommand(params))
                            .then(() => setTimeout(() => next(), azureTimeout))
                            .catch(next);
                    },
                    next => {
                        setTimeout(() => {
                            azureGetCheck(this.test.keyName, normalMD5,
                              azureMetadata, next);
                        }, azureTimeout);
                    },
                ], done);
            });

            it('should put objects with same key to Azure ' +
            'then file, and object should only be present in file', function
            itF(done) {
                const params = {
                    Bucket: azureContainerName,
                    Key: this.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': azureLocation } };
                async.waterfall([
                    next => {
                        s3.send(new PutObjectCommand(params))
                            .then(() => next())
                            .catch(next);
                    },
                    next => {
                        params.Metadata = { 'scal-location-constraint':
                        fileLocation };
                        s3.send(new PutObjectCommand(params))
                            .then(() => setTimeout(() => next(), azureTimeout))
                            .catch(next);
                    },
                    next => {
                        s3.send(new GetObjectCommand({
                            Bucket: azureContainerName,
                            Key: this.test.keyName,
                        }))
                            .then(res => {
                                assert.strictEqual(
                                    res.Metadata['scal-location-constraint'],
                                    fileLocation);
                                next();
                            })
                            .catch(next);
                    },
                    next => {
                        azureClient.getContainerClient(azureContainerName)
                            .getBlobClient(this.test.keyName).getProperties()
                            .then(() => {
                                next(new Error('Expected NotFound error'));
                            })
                            .catch(err => {
                                assert.strictEqual(err.code, 'NotFound');
                                next();
                            });
                    },
                ], done);
            });

            it('should put objects with same key to file ' +
            'then Azure, and object should only be present on Azure',
            function itF(done) {
                const params = { Bucket: azureContainerName, Key:
                    this.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                async.waterfall([
                    next => s3.send(new PutObjectCommand(params)).then(() => next()),
                    next => {
                        params.Metadata = {
                            'scal-location-constraint': azureLocation,
                        };
                        s3.send(new PutObjectCommand(params)).then(() => setTimeout(() =>
                          next(), azureTimeout));
                    },
                    next => azureGetCheck(this.test.keyName, normalMD5,
                      azureMetadata, next),
                ], done);
            });

            describe('with ongoing MPU with same key name', () => {
                beforeEach(function beFn(done) {
                    s3.send(new CreateMultipartUploadCommand({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }))
                        .then(res => {
                            this.currentTest.uploadId = res.UploadId;
                            done();
                        })
                        .catch(done);
                });

                afterEach(function afFn(done) {
                    s3.send(new AbortMultipartUploadCommand({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyName,
                        UploadId: this.currentTest.uploadId,
                    }))
                        .then(() => {
                            done();
                        })
                        .catch(done);
                });

                it('should return ServiceUnavailable', function itFn(done) {
                    s3.send(new PutObjectCommand({
                        Bucket: azureContainerName,
                        Key: this.test.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }))
                        .then(() => {
                            done(new Error('Expected ServiceUnavailable error'));
                        })
                        .catch(err => {
                            assert.strictEqual(err.name, 'ServiceUnavailable');
                            done();
                        });
                });
            });
        });
    });
});
