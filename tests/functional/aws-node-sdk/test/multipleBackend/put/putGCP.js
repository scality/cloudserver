const assert = require('assert');
const async = require('async');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const { config } = require('../../../../../../lib/Config');
const { uniqName, getGcpClient, getGcpBucketName, getGcpKeys, convertMD5 }
    = require('../utilsGCP');

const gcpLocation = 'gcp-test';
const keyObject = 'putgcp';
const gcpBucket = getGcpBucketName();
const gcpClient = getGcpClient();
const gcpKeys = getGcpKeys();
const { versioningEnabled } = require('../../../lib/utility/versioning-util');

const normalBody = Buffer.from('I am a body', 'utf8');
const normalMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

/* eslint-disable camelcase */
const gcpMetadata = {
    'scal-location-constraint': gcpLocation,
};
/* eslint-enable camelcase */

const gcpTimeout = 20000;
let bucketUtil;
let s3;

function gcpGetCheck(objectKey, gcpMD5, gcpMetadata, cb) {
    const bucket = gcpClient.bucket(gcpBucket);
    const file = bucket.file(objectKey);

    file.getMetadata((err, metadata, res) => {
        assert.strictEqual(err, null, 'Expected success, got error ' +
        `on call to GCP: ${err}`);
        const resMD5 = convertMD5(metadata.md5Hash);
        assert.strictEqual(resMD5, gcpMD5);
        if (!res.metadata) {
            assert.deepStrictEqual({}, gcpMetadata);
        } else {
            assert.deepStrictEqual(res.metadata, gcpMetadata);
        }
        return cb();
    });
}

describeSkipIfNotMultiple('MultipleBackend put object to GCP', function
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
            return bucketUtil.empty(gcpBucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(gcpBucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });
        describe('with bucket location header', () => {
            beforeEach(done =>
              s3.createBucket({ Bucket: gcpBucket,
                  CreateBucketConfiguration: {
                      LocationConstraint: gcpLocation,
                  },
              }, done));
            it('should put an object to GCP, with no object location ' +
            'header, based on bucket location', function it(done) {
                const params = {
                    Bucket: gcpBucket,
                    Key: this.test.keyName,
                    Body: normalBody,
                };
                async.waterfall([
                    next => s3.putObject(params, err => setTimeout(() =>
                      next(err), gcpTimeout)),
                    next => gcpGetCheck(this.test.keyName, normalMD5, {},
                      next),
                ], done);
            });
        });

        describe('with no bucket location header', () => {
            beforeEach(() =>
              s3.createBucketAsync({ Bucket: gcpBucket })
                .catch(err => {
                    process.stdout.write(`Error creating bucket: ${err}\n`);
                    throw err;
                }));

            gcpKeys.forEach(key => {
                it(`should put a ${key.describe} object to GCP`,
                function itF(done) {
                    const params = {
                        Bucket: gcpBucket,
                        Key: this.test.keyName,
                        Metadata: { 'scal-location-constraint': gcpLocation },
                        Body: key.body,
                    };
                    s3.putObject(params, err => {
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        setTimeout(() =>
                            gcpGetCheck(this.test.keyName,
                              key.MD5, gcpMetadata,
                            () => done()), gcpTimeout);
                    });
                });
            });

            it('should return error NotImplemented putting a ' +
            'version to GCP', function itF(done) {
                s3.putBucketVersioning({
                    Bucket: gcpBucket,
                    VersioningConfiguration: versioningEnabled,
                }, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    const params = { Bucket: gcpBucket,
                        Key: this.test.keyName,
                        Body: normalBody,
                        Metadata: { 'scal-location-constraint':
                        gcpLocation } };
                    s3.putObject(params, err => {
                        assert.strictEqual(err.code, 'NotImplemented');
                        done();
                    });
                });
            });

            it('should put two objects to GCP with same ' +
            'key, and newest object should be returned', function itF(done) {
                const params = {
                    Bucket: gcpBucket,
                    Key: this.test.keyName,
                    Metadata: { 'scal-location-constraint': gcpLocation },
                };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Body = normalBody;
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), gcpTimeout));
                    },
                    next => {
                        setTimeout(() => {
                            gcpGetCheck(this.test.keyName, normalMD5,
                              gcpMetadata, next);
                        }, gcpTimeout);
                    },
                ], done);
            });

            it('should put objects with same key to GCP ' +
            'then file, and object should only be present in file', function
            itF(done) {
                const params = {
                    Bucket: gcpBucket,
                    Key: this.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = { 'scal-location-constraint':
                        'file' };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), gcpTimeout));
                    },
                    next => s3.getObject({
                        Bucket: gcpBucket,
                        Key: this.test.keyName,
                    }, (err, res) => {
                        assert.equal(err, null, 'Expected success, ' +
                            `got error ${err}`);
                        assert.strictEqual(
                            res.Metadata['scal-location-constraint'],
                            'file');
                        next();
                    }),
                    next => {
                        const bucket = gcpClient.bucket(gcpBucket);
                        const file = bucket.file(this.test.keyName);

                        file.getMetadata(err => {
                            assert.strictEqual(err.code, 404);
                            next();
                        });
                    },
                ], done);
            });

            it('should put objects with same key to file ' +
            'then GCP, and object should only be present on GCP',
            function itF(done) {
                const params = { Bucket: gcpBucket, Key:
                    this.test.keyName,
                    Body: normalBody,
                    Metadata: { 'scal-location-constraint': 'file' } };
                async.waterfall([
                    next => s3.putObject(params, err => next(err)),
                    next => {
                        params.Metadata = {
                            'scal-location-constraint': gcpLocation,
                        };
                        s3.putObject(params, err => setTimeout(() =>
                          next(err), gcpTimeout));
                    },
                    next => gcpGetCheck(this.test.keyName, normalMD5,
                      gcpMetadata, next),
                ], done);
            });
        });
    });
});
