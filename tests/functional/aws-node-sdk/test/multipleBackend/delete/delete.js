const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultiple,
    memLocation,
    fileLocation,
    awsLocation,
    awsLocationMismatch,
    genUniqID,
} = require('../utils');

const bucket = `deleteaws${genUniqID()}`;
const memObject = `memObject-${genUniqID()}`;
const fileObject = `fileObject-${genUniqID()}`;
const awsObject = `awsObject-${genUniqID()}`;
const emptyObject = `emptyObject-${genUniqID()}`;
const bigObject = `bigObject-${genUniqID()}`;
const mismatchObject = `mismatchOjbect-${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);

describeSkipIfNotMultiple('Multiple backend delete', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            })
            .then(() => {
                process.stdout.write('Putting object to mem\n');
                const params = { Bucket: bucket, Key: memObject, Body: body,
                    Metadata: { 'scal-location-constraint': memLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to file\n');
                const params = { Bucket: bucket, Key: fileObject, Body: body,
                    Metadata: { 'scal-location-constraint': fileLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to AWS\n');
                const params = { Bucket: bucket, Key: awsObject, Body: body,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting 0-byte object to AWS\n');
                const params = { Bucket: bucket, Key: emptyObject,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting large object to AWS\n');
                const params = { Bucket: bucket, Key: bigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': awsLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to AWS\n');
                const params = { Bucket: bucket, Key: mismatchObject,
                    Body: body, Metadata:
                    { 'scal-location-constraint': awsLocationMismatch } };
                return s3.putObjectAsync(params);
            })
            .catch(err => {
                process.stdout.write(`Error putting objects: ${err}\n`);
                throw err;
            });
        });
        after(() => {
            process.stdout.write('Deleting bucket\n');
            return bucketUtil.deleteOne(bucket)
            .catch(err => {
                process.stdout.write(`Error deleting bucket: ${err}\n`);
                throw err;
            });
        });

        it('should delete object from mem', done => {
            s3.deleteObject({ Bucket: bucket, Key: memObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: memObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from file', done => {
            s3.deleteObject({ Bucket: bucket, Key: fileObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: fileObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: awsObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: awsObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete 0-byte object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: emptyObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: emptyObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete large object from AWS', done => {
            s3.deleteObject({ Bucket: bucket, Key: bigObject }, err => {
                assert.strictEqual(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: bigObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey', 'Expected ' +
                        'error but got success');
                    done();
                });
            });
        });
        it('should delete object from AWS location with bucketMatch set to ' +
        'false', done => {
            s3.deleteObject({ Bucket: bucket, Key: mismatchObject }, err => {
                assert.equal(err, null,
                    `Expected success, got error ${JSON.stringify(err)}`);
                s3.getObject({ Bucket: bucket, Key: mismatchObject }, err => {
                    assert.strictEqual(err.code, 'NoSuchKey',
                        'Expected error but got success');
                    done();
                });
            });
        });
    });
});
