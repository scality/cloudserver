const assert = require('assert');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const { config } = require('../../../../../../lib/Config');

const bucket = 'buckettestmultiplebackenddelete';
const memObject = `memObject-${Date.now()}`;
const fileObject = `fileObject-${Date.now()}`;
const awsObject = `awsObject-${Date.now()}`;
const emptyObject = `emptyObject-${Date.now()}`;
const bigObject = `bigObject-${Date.now()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

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
                    Metadata: { 'scal-location-constraint': 'mem' } };
                return s3.putObject(params);
            })
            .then(() => {
                process.stdout.write('Putting object to file\n');
                const params = { Bucket: bucket, Key: fileObject, Body: body,
                    Metadata: { 'scal-location-constraint': 'file' } };
                return s3.putObject(params);
            })
            .then(() => {
                process.stdout.write('Putting object to AWS\n');
                const params = { Bucket: bucket, Key: awsObject, Body: body,
                    Metadata: { 'scal-location-constraint': 'aws-test' } };
                return s3.putObject(params);
            })
            .then(() => {
                process.stdout.write('Putting 0-byte object to AWS\n');
                const params = { Bucket: bucket, Key: emptyObject,
                    Metadata: { 'scal-location-constraint': 'aws-test' } };
                return s3.putObject(params);
            })
            .then(() => {
                process.stdout.write('Putting large object to AWS\n');
                const params = { Bucket: bucket, Key: bigObject,
                    Body: bigBody,
                    Metadata: { 'scal-location-constraint': 'aws-test' } };
                return s3.putObject(params);
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
    });
});
