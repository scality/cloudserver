const assert = require('assert');
const AWS = require('aws-sdk');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { getRealAwsConfig } = require('../support/awsConfig');
const { config } = require('../../../../../lib/Config');

const bucket = 'buckettestmultiplebackendget';
const awsBucket = 'multitester555';
const memObject = 'memobject';
const fileObject = 'fileobject';
const awsObject = 'awsobject';
const emptyObject = 'emptyObject';
const emptyAwsObject = 'emptyObject';
const bigObject = 'bigObject';
const body = Buffer.from('I am a body', 'utf8');
const bigBody = new Buffer(10485760);
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigMD5 = 'f1c9645dbc14efddc7d8a322685f26eb';

let awsS3;

const describeSkipIfNotMultiple = (config.backends.data !== 'multiple'
    || process.env.S3_END_TO_END) ? describe.skip : describe;

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

        describeSkipIfNotMultiple('with objects in all available backends ' +
            '(mem/file/AWS)', () => {
            before(() => {
                const awsConfig = getRealAwsConfig('default');
                awsS3 = new AWS.S3(awsConfig);

                process.stdout.write('Putting object to mem\n');
                return s3.putObjectAsync({ Bucket: bucket, Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'mem' } })
                .then(() => {
                    process.stdout.write('Putting object to file\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: fileObject,
                        Body: body,
                        Metadata: { 'scal-location-constraint': 'file' } });
                })
                .then(() => {
                    process.stdout.write('Putting object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: awsObject,
                        Body: body,
                        Metadata: { 'scal-location-constraint': 'aws-test' } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to mem\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: emptyObject,
                        Metadata: { 'scal-location-constraint': 'mem' } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: emptyAwsObject,
                        Metadata: { 'scal-location-constraint': 'aws-test' } });
                })
                .then(() => {
                    process.stdout.write('Putting large object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: bigObject, Body: bigBody,
                        Metadata: { 'scal-location-constraint': 'aws-test' } });
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
                s3.getObject({ Bucket: bucket, Key: emptyObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                    done();
                });
            });
            it('should get a 0-byte object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: emptyAwsObject },
                (err, res) => {
                    assert.equal(err, null, 'Expected success but got error ' +
                        `error ${err}`);
                    assert.strictEqual(res.ETag, `"${emptyMD5}"`);
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
            it('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: awsObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
            });
            it('should get a large object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: bigObject },
                    (err, res) => {
                        assert.equal(err, null, 'Expected success but got ' +
                            `error ${err}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    });
            });
            it('should return an error on get done to object deleted from AWS',
            done => {
                awsS3.deleteObject({ Bucket: awsBucket, Key: awsObject },
                err => {
                    assert.equal(err, null, 'Expected success but got ' +
                        `error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: awsObject }, err => {
                        assert.strictEqual(err.code, 'NetworkingError');
                        done();
                    });
                });
            });
        });
    });
});
