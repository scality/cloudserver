const assert = require('assert');
const async = require('async');
const { CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    GetObjectCommand,
    CreateMultipartUploadCommand,
    AbortMultipartUploadCommand } = require('@aws-sdk/client-s3');
const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const {
    uniqName,
    getAzureClient,
    getAzureContainerName,
    getAzureKeys,
    azureLocation,
    azureLocationMismatch,
    describeSkipIfNotMultiple,
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

describeSkipIfNotMultiple('Multiple backend delete object from Azure',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.send(new CreateBucketCommand({ Bucket: azureContainerName }))
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
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
                before(done => {
                    s3.send(new PutObjectCommand({
                        Bucket: azureContainerName,
                        Key: keyName,
                        Body: key.body,
                        Metadata: {
                            'scal-location-constraint': azureLocation,
                        },
                    })).then(() => done());
                });

                it(`should delete an ${key.describe} object from Azure`,
                done => {
                    s3.send(new DeleteObjectCommand({
                        Bucket: azureContainerName,
                        Key: keyName,
                    })).then(() => {
                        setTimeout(() => azureClient.getContainerClient(azureContainerName)
                            .getProperties(keyName)
                            .then(() => assert.fail('Expected error'), err => {
                                assert.strictEqual(err.statusCode, 404);
                                assert.strictEqual(err.code, 'NotFound');
                                return done();
                            }), azureTimeout);
                    }).catch(err => {
                        assert.equal(err, null, 'Expected success ' +
                            `but got error ${err}`);
                    });
                });
            });
        });

        describe('delete from Azure location with bucketMatch set to false',
        () => {
            beforeEach(function beforeF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.send(new PutObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocationMismatch,
                    },
                })).then(() => done());
            });

            it('should delete object', function itF(done) {
                s3.send(new DeleteObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.test.azureObject,
                })).then(() => {
                    setTimeout(() =>
                    azureClient.getContainerClient(azureContainerName)
                        .getProperties(`${azureContainerName}/${this.test.azureObject}`)
                        .then(() => assert.fail('Expected error'), err => {
                            assert.strictEqual(err.statusCode, 404);
                            assert.strictEqual(err.code, 'NotFound');
                            return done();
                        }), azureTimeout);
                }).catch(err => {
                    assert.equal(err, null, 'Expected success ' +
                        `but got error ${err}`);
                });
            });
        });

        describe('returning no error', () => {
            beforeEach(function beF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.send(new PutObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                })).then(() => {
                    azureClient.getContainerClient(azureContainerName)
                        .deleteBlob(this.currentTest.azureObject).then(done, err => {
                            assert.equal(err, null, 'Expected success but got ' +
                                `error ${err}`);
                            done(err);
                        });
                }).catch(err => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    done();
                });
            });

            it('should return no error on deleting an object deleted ' +
            'from Azure', function itF(done) {
                s3.send(new DeleteObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.test.azureObject,
                })).then(() => {
                    done();
                }).catch(err => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    done();
                });
            });
        });

        describe('Versioning:: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.azureObject = uniqName(keyObject);
                s3.send(new PutObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.currentTest.azureObject,
                    Body: normalBody,
                    Metadata: {
                        'scal-location-constraint': azureLocation,
                    },
                })).then(() => done());
            });

            it('should not delete object when deleting a non-existing ' +
            'version from Azure', function itF(done) {
                async.waterfall([
                    next => s3.send(new DeleteObjectCommand({
                        Bucket: azureContainerName,
                        Key: this.test.azureObject,
                        VersionId: nonExistingId,
                    })).then(() => next())
                    .catch(err => {
                        next(err);
                    }),
                    next => s3.send(new GetObjectCommand({
                        Bucket: azureContainerName,
                        Key: this.test.azureObject,
                    })).then(res => {
                        assert.deepStrictEqual(res.Body, normalBody);
                        return next();
                    }).catch(err => {
                        assert.equal(err, null, 'getObject: Expected success ' +
                            `but got error ${err}`);
                        next(err);
                    }),
                    next => azureClient.getContainerClient(azureContainerName)
                    .getBlobClient(this.test.azureObject)
                    .downloadToBuffer().then(res => {
                        assert.deepStrictEqual(res, normalBody);
                        return next();
                    }, err => {
                        assert.equal(err, null, 'getBlobToText: Expected ' +
                            `successbut got error ${err}`);
                        return next();
                    }),
                ], done);
            });
        });

        describe('with ongoing MPU: ', () => {
            beforeEach(function beF(done) {
                this.currentTest.key = uniqName(keyObject);
                const params = {
                    Bucket: azureContainerName,
                    Key: this.currentTest.key,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': azureLocation },
                };
                s3.send(new PutObjectCommand(params)).then(() => {
                    const params = {
                        Bucket: azureContainerName,
                        Key: this.currentTest.key,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    };
                    s3.send(new CreateMultipartUploadCommand(params)).then(res => {
                        this.currentTest.uploadId = res.UploadId;
                        setTimeout(() => done(), azureTimeout);
                    }).catch(err => {
                        assert.equal(err, null, 'Err initiating MPU on ' +
                            `Azure: ${err}`);
                        done();
                    });
                }).catch(err => {
                    assert.equal(err, null, 'Err putting object to Azure: ' +
                        `${err}`);
                    done();
                });
            });

            afterEach(function afF(done) {
                s3.send(new AbortMultipartUploadCommand({
                    Bucket: azureContainerName,
                    Key: this.currentTest.key,
                    UploadId: this.currentTest.uploadId,
                })).then(() => {
                    setTimeout(() => done(), azureTimeout);
                }).catch(err => {
                    assert.equal(err, null, `Err aborting MPU: ${err}`);
                    done();
                });
            });

            it('should return InternalError', function itFn(done) {
                s3.send(new DeleteObjectCommand({
                    Bucket: azureContainerName,
                    Key: this.test.key,
                })).then(() => {
                    done();
                }).catch(err => {
                    assert.strictEqual(err.code, 'MPUinProgress');
                    done();
                });
            });
        });
    });
});
