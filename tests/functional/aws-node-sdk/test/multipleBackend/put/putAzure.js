const assert = require('assert');
const async = require('async');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
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
        assert.strictEqual(err, null, 'Expected success, got error ' +
        `on call to Azure: ${err}`);
        const resMD5 = convertMD5(res.contentSettings.contentMD5);
        assert.strictEqual(resMD5, azureMD5);
        assert.deepStrictEqual(res.metadata, azureMetadata);
        return cb();
    });
}

describeSkipIfNotMultiple('MultipleBackend put object to AZURE', function
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
            beforeEach(done =>
                s3.createBucket({ Bucket: azureContainerName,
                    CreateBucketConfiguration: {
                        LocationConstraint: azureLocation,
                    },
                }, done));

            it('should return a NotImplemented error if try to put ' +
            'versioning to bucket with Azure location', done => {
                const params = {
                    Bucket: azureContainerName,
                    VersioningConfiguration: {
                        Status: 'Enabled',
                    },
                };
                s3.putBucketVersioning(params, err => {
                    assert.strictEqual(err.code, 'NotImplemented');
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
                    next => s3.putObject(params, err => setTimeout(() =>
                      next(err), azureTimeout)),
                    next => azureGetCheck(this.test.keyName, normalMD5, {},
                      next),
                ], done);
            });
        });

        describe('with no bucket location header', () => {
            beforeEach(() =>
              s3.createBucketAsync({ Bucket: azureContainerName })
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
                    s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        setTimeout(() =>
                            azureGetCheck(this.test.keyName,
                              key.MD5, azureMetadata,
                            () => done()), azureTimeout);
                    });
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
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                    `got error ${err}`);
                    setTimeout(() =>
                        azureGetCheck(
                          `${azureContainerName}/${this.test.keyName}`,
                          normalMD5, azureMetadataMismatch,
                        () => done()), azureTimeout);
                });
            });

            it('should return error ServiceUnavailable putting an invalid ' +
            'key name to Azure', done => {
                const params = {
                    Bucket: azureContainerName,
                    Key: '.',
                    Metadata: { 'scal-location-constraint': azureLocation },
                    Body: normalBody,
                };
                s3.putObject(params, err => {
                    assert.strictEqual(err.code, 'ServiceUnavailable');
                    done();
                });
            });

            it('should return error NotImplemented putting a ' +
            'version to Azure', function itF(done) {
                s3.putBucketVersioning({
                    Bucket: azureContainerName,
                    VersioningConfiguration: versioningEnabled,
                }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    const params = { Bucket: azureContainerName,
                        Key: this.test.keyName,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        azureLocation } };
                    s3.putObject(params, err => {
                        assert.strictEqual(err.code, 'NotImplemented');
                        done();
                    });
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
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Body = normalBody;
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
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
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = { 'scal-location-constraint':
                        fileLocation };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
                    },
                    next => s3.getObject({
                        Bucket: azureContainerName,
                        Key: this.test.keyName,
                    }, (err, res) => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        assert.strictEqual(
                            res.Metadata['scal-location-constraint'],
                            fileLocation);
                        next();
                    }),
                    next => azureClient.getBlobProperties(azureContainerName,
                    this.test.keyName, err => {
                        assert.strictEqual(err.code, 'NotFound');
                        next();
                    }),
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
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = {
                            'scal-location-constraint': azureLocation,
                        };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), azureTimeout));
                    },
                    next => azureGetCheck(this.test.keyName, normalMD5,
                      azureMetadata, next),
                ], done);
            });

            describe('with ongoing MPU with same key name', () => {
                beforeEach(function beFn(done) {
                    s3.createMultipartUpload({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, (err, res) => {
                        assert.equal(err, null, `Err creating MPU: ${err}`);
                        this.currentTest.uploadId = res.UploadId;
                        done();
                    });
                });

                afterEach(function afFn(done) {
                    s3.abortMultipartUpload({
                        Bucket: azureContainerName,
                        Key: this.currentTest.keyName,
                        UploadId: this.currentTest.uploadId,
                    }, err => {
                        assert.equal(err, null, `Err aborting MPU: ${err}`);
                        done();
                    });
                });

                it('should return ServiceUnavailable', function itFn(done) {
                    s3.putObject({
                        Bucket: azureContainerName,
                        Key: this.test.keyName,
                        Metadata: { 'scal-location-constraint': azureLocation },
                    }, err => {
                        assert.strictEqual(err.code, 'ServiceUnavailable');
                        done();
                    });
                });
            });
        });
    });
});
