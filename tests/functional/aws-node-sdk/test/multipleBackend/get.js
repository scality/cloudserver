const assert = require('assert');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'buckettestmultiplebackendget';
const memObject = 'memobject';
const fileObject = 'fileobject';
const emptyObject = 'emptyObject';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

const describeSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

describe('Multiple backend get object', () => {
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

        it('should return an error to get request without a valid bucket name',
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
                s3.getObject({ Bucket: bucket, Key: 'nope' }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'NoSuchKey');
                    done();
                });
            });

        describeSkipIfE2E('with objects in all available backends ' +
            '(mem/file)', () => {
            before(() => {
                process.stdout.write('Putting object to mem');
                return s3.putObjectAsync({ Bucket: bucket, Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'mem' } })
                .then(() => {
                    process.stdout.write('Putting object to file');
                    return s3.putObjectAsync({ Bucket: bucket, Key: fileObject,
                        Body: body,
                        Metadata: { 'scal-location-constraint': 'file' } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to mem');
                    return s3.putObjectAsync({ Bucket: bucket, Key: emptyObject,
                        Metadata: { 'scal-location-constraint': 'mem' } });
                })
                .catch(err => {
                    process.stdout.write(`Error putting objects: ${err}\n`);
                    throw err;
                });
            });
            it('should get an object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: memObject }, (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${correctMD5}"`);
                    done();
                });
            });
            it('should get a 0-byte object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: emptyObject }, err => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    done();
                });
            });
            it('should get an object from file', done => {
                s3.getObject({ Bucket: bucket, Key: fileObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
        });
    });
});
