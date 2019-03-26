const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const {
    describeSkipIfNotMultipleOrCeph,
    gcpLocation,
    gcpLocationMismatch,
    genUniqID,
} = require('../utils');

const bucket = `deletegcp${genUniqID()}`;
const gcpObject = `gcpObject-${genUniqID()}`;
const emptyObject = `emptyObject-${genUniqID()}`;
const bigObject = `bigObject-${genUniqID()}`;
const mismatchObject = `mismatchObject-${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);

describeSkipIfNotMultipleOrCeph('Multiple backend delete',
function testSuite() {
    this.timeout(120000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeAll(() => {
            process.stdout.write('Creating bucket\n');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            }).then(() => {
                process.stdout.write('Putting object to GCP\n');
                const params = { Bucket: bucket, Key: gcpObject, Body: body,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting 0-byte object to GCP\n');
                const params = { Bucket: bucket, Key: emptyObject,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting large object to GCP\n');
                const params = { Bucket: bucket, Key: bigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': gcpLocation } };
                return s3.putObjectAsync(params);
            })
            .then(() => {
                process.stdout.write('Putting object to GCP\n');
                const params = { Bucket: bucket, Key: mismatchObject,
                    Body: body, Metadata:
                    { 'scal-location-constraint': gcpLocationMismatch } };
                return s3.putObjectAsync(params);
            })
            .catch(err => {
                process.stdout.write(`Error putting objects: ${err}\n`);
                throw err;
            });
        });
        afterAll(() => {
            process.stdout.write('Deleting bucket\n');
            return bucketUtil.deleteOne(bucket)
            .catch(err => {
                process.stdout.write(`Error deleting bucket: ${err}\n`);
                throw err;
            });
        });

        const deleteTests = [
            {
                msg: 'should delete object from GCP',
                Bucket: bucket, Key: gcpObject,
            },
            {
                msg: 'should delete 0-byte object from GCP',
                Bucket: bucket, Key: emptyObject,
            },
            {
                msg: 'should delete large object from GCP',
                Bucket: bucket, Key: bigObject,
            },
            {
                msg: 'should delete object from GCP location with ' +
                    'bucketMatch set to false',
                Bucket: bucket, Key: mismatchObject,
            },
        ];
        deleteTests.forEach(test => {
            const { msg, Bucket, Key } = test;
            test(msg, done => s3.deleteObject({ Bucket, Key }, err => {
                expect(err).toBe(null);
                s3.getObject({ Bucket, Key }, err => {
                    expect(err.code).toBe('NoSuchKey');
                    done();
                });
            }));
        });

        test(
            'should return success if the object does not exist',
            done => s3.deleteObject({ Bucket: bucket, Key: 'noop' }, err => {
                expect(err).toBe(null);
                done();
            })
        );
    });
});
