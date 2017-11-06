const assert = require('assert');
const async = require('async');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const { config } = require('../../../../../../lib/Config');
const { uniqName, getGcpClient, getGcpBucketName, getGcpKeys }
    = require('../utilsGCP');

const keyObject = 'deletegcp';
const gcpLocation = 'gcp-test';
const keys = getGcpKeys();
const gcpClient = getGcpClient();
const gcpBucketName = getGcpBucketName();

const normalBody = Buffer.from('I am a body', 'utf8');
const gcpTimeout = 20000;

const gcpMetadata = {
    'scal-location-constraint': gcpLocation,
};

const nonExistingId = process.env.AWS_ON_AIR ?
    'MhhyTHhmZ4cxSi4Y9SMe5P7UJAz7HLJ9' :
    '3939393939393939393936493939393939393939756e6437';

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

describeSkipIfNotMultiple('Multiple backend delete object from GCP',
function testSuite() {
    this.timeout(250000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: gcpBucketName })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(gcpBucketName)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(gcpBucketName);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
            });
        });

        keys.forEach(key => {
            const keyName = uniqName(keyObject);
            describe(`${key.describe} size`, () => {
                before(done => {
                    s3.putObject({
                        Bucket: gcpBucketName,
                        Key: keyName,
                        Body: key.body,
                        Metadata: gcpMetadata,
                    }, done);
                });

                it(`should delete an ${key.describe} object from GCP`,
                done => {
                    s3.deleteObject({
                        Bucket: gcpBucketName,
                        Key: keyName,
                    }, err => {
                        assert.equal(err, null, 'Expected success ' +
                            `but got error ${err}`);
                        setTimeout(() => {
                            const bucket = gcpClient.bucket(gcpBucketName);
                            const file = bucket.file(keyName);

                            file.download(err => {
                                assert.strictEqual(err.code, 404);
                                assert.strictEqual(err.message,
                                    'Not Found');
                                return done();
                            });
                        }, gcpTimeout);
                    });
                });
            });
        });

        describe('returning no error', () => {
            const bucket = gcpClient.bucket(gcpBucketName);
            beforeEach(function beF(done) {
                this.currentTest.gcpObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: gcpBucketName,
                    Key: this.currentTest.gcpObject,
                    Body: normalBody,
                    Metadata: gcpMetadata,
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
                    this.currentTest.file
                        = bucket.file(this.currentTest.gcpObject);
                    this.currentTest.file.exists((err, exists) => {
                        assert.equal(err, null, 'Expected success ' +
                            `but got error ${err}`);
                        assert.strictEqual(exists, true, 'Expected file exist' +
                            'to be true but got false');
                        this.currentTest.file.delete(err => {
                            assert.equal(err, null, 'Expected success ' +
                                `but got error ${err}`);
                            done(err);
                        });
                    });
                });
            });

            it('should return no error on deleting an object deleted ' +
            'from GCP', function ifF(done) {
                s3.deleteObject({
                    Bucket: gcpBucketName,
                    Key: this.test.gcpObject,
                }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                    `error ${err}`);
                    done();
                });
            });
        });

        describe('Versioning:: ', () => {
            const bucket = gcpClient.bucket(gcpBucketName);
            beforeEach(function beF(done) {
                this.currentTest.gcpObject = uniqName(keyObject);
                s3.putObject({
                    Bucket: gcpBucketName,
                    Key: this.currentTest.gcpObject,
                    Body: normalBody,
                    Metadata: gcpMetadata,
                }, err => {
                    assert.equal(err, null, 'Expected success ' +
                        `but got error ${err}`);
                    this.currentTest.file
                        = bucket.file(this.currentTest.gcpObject);
                    done();
                });
            });

            it('should not delete object when deleting a non-existing ' +
            'version from GCP', function itF(done) {
                async.waterfall([
                    next => s3.deleteObject({
                        Bucket: gcpBucketName,
                        Key: this.test.gcpObject,
                        VersionId: nonExistingId,
                    }, err => next(err)),
                    next => s3.getObject({
                        Bucket: gcpBucketName,
                        Key: this.test.gcpObject,
                    }, (err, res) => {
                        assert.equal(err, null, 'getObject: Expected success ' +
                        `but got error ${err}`);
                        assert.deepStrictEqual(res.Body, normalBody);
                        return next(err);
                    }),
                    next => {
                        this.test.file.exists((err, exists) => {
                            assert.equal(err, null, 'Expected success ' +
                                `but got error ${err}`);
                            assert.strictEqual(exists, true,
                                'Expected file exist to be true but got false');
                            this.test.file.download((err, res) => {
                                assert.equal(err, null, 'Expected success ' +
                                    `but got error ${err}`);
                                assert.deepStrictEqual(Buffer.from(res, 'utf8'),
                                    normalBody);
                                return next();
                            });
                        });
                    },
                ], done);
            });
        });
    });
});
