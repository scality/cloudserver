const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');
const { config } = require('../../../../../../lib/Config');
const { uniqName, getGcpClient, getGcpBucketName, getGcpKeys }
    = require('../utilsGCP');

const gcpLocation = 'gcp-test';
const gcpBucket = getGcpBucketName();
const gcpClient = getGcpClient();
const gcpKeys = getGcpKeys();
const keyObject = 'getgcp';

const normalBody = Buffer.from('I am a body', 'utf8');

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

/* eslint-disable camelcase */
const gcpMetadata = {
    'scal-location-constraint': gcpLocation,
};
/* eslint-enable camelcase */

const gcpTimeout = 1000;

describeSkipIfNotMultiple('Multiple backend get object from GCP',
function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: gcpBucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(gcpBucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(gcpBucket);
            })
            .catch(err => {
                process.stdout.write(
                    'Error emptying/deleting bucket: ' + `${err}\n`);
                throw err;
            });
        });

        gcpKeys.forEach(key => {
            describe(`${key.describe} size`, () => {
                const testKey = `${key.name}-${Date.now()}`;
                before(done => {
                    setTimeout(() => {
                        s3.putObject({
                            Bucket: gcpBucket,
                            Key: testKey,
                            Body: key.body,
                            Metadata: gcpMetadata,
                        }, done);
                    }, gcpTimeout);
                });

                it(`should get an ${key.describe} object from GCP`, done => {
                    s3.getObject({ Bucket: gcpBucket, Key: testKey },
                    (err, res) => {
                        assert.equal(err, null,
                            'Expected success ' + `but got error ${err}`);
                        assert.strictEqual(res.ETag, `"${key.MD5}"`);
                        done();
                    });
                });
            });
        });

        describe('returning error', () => {
            const gcpObject = uniqName(keyObject);
            before(done => {
                s3.putObject({
                    Bucket: gcpBucket,
                    Key: gcpObject,
                    Body: normalBody,
                    Metadata: gcpMetadata,
                }, err => {
                    assert.equal(err, null,
                        `Expected success but got error ${err}`);
                    const bucket = gcpClient.bucket(gcpBucket);
                    const file = bucket.file(gcpObject);

                    file.exists((err, exists) => {
                        if (!err && exists) {
                            file.delete(err => {
                                assert.equal(err, null,
                                    `Expected success but got error ${err}`);
                                done();
                            });
                        } else {
                            process.stdout.write(
                                `Error deleting key: ${err}\n`);
                            throw err;
                        }
                    });
                });
            });

            it('should return an error on get done to object deleted from GCP',
            done => {
                s3.getObject({ Bucket: gcpBucket, Key: gcpObject },
                err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NetworkingError');
                    done();
                });
            });

            it('should return an error to get request an invalid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });

            it('should return NoSuchKey error when no such object',
            done => {
                s3.getObject({ Bucket: gcpBucket, Key: 'nope' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchKey');
                    done();
                });
            });
        });
    });
});
