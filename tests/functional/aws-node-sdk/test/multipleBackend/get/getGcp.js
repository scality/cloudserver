const assert = require('assert');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    gcpLocation,
    gcpLocationMismatch,
} = require('../utils');

const bucket = 'buckettestmultiplebackendget-gcp';
const gcpObject = `gcpobject-${Date.now()}`;
const emptyGcpObject = `emptyObject-${Date.now()}`;
const bigObject = `bigObject-${Date.now()}`;
const mismatchObject = `mismatch-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const bigBodyLen = bigBody.length;
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigMD5 = 'f1c9645dbc14efddc7d8a322685f26eb';

describe('Multiple backend get object', function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error emptying/deleting bucket: ' +
                `${err}\n`);
                throw err;
            });
        });

        describeSkipIfNotMultiple('with objects in GCP', () => {
            before(() => {
                process.stdout.write('Putting object to GCP\n');
                return s3.putObjectAsync({ Bucket: bucket, Key: gcpObject,
                    Body: body,
                    Metadata: {
                        'scal-location-constraint': gcpLocation },
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to GCP\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: emptyGcpObject,
                        Metadata: {
                            'scal-location-constraint': gcpLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting large object to GCP\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: bigObject, Body: bigBody,
                        Metadata: {
                            'scal-location-constraint': gcpLocation } });
                })
                .catch(err => {
                    process.stdout.write(`Error putting objects: ${err}\n`);
                    throw err;
                });
            });
            it('should get a 0-byte object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: emptyGcpObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                    done();
                });
            });
            it('should get an object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: gcpObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
            it('should get a large object from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: bigObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
            it('should get an object using range query from GCP', done => {
                s3.getObject({ Bucket: bucket, Key: bigObject,
                    Range: 'bytes=0-9' },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ContentLength, '10');
                        assert.strictEqual(res.ContentRange,
                            `bytes 0-9/${bigBodyLen}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
        });

        describeSkipIfNotMultiple('with bucketMatch set to false', () => {
            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: mismatchObject, Body: body,
                Metadata: { 'scal-location-constraint': gcpLocationMismatch } },
                err => {
                    assert.equal(err, null, `Err putting object: ${err}`);
                    done();
                });
            });

            it('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: mismatchObject },
                (err, res) => {
                    assert.equal(err, null, `Error getting object: ${err}`);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    done();
                });
            });
        });
    });
});
