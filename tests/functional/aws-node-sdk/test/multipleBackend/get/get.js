const assert = require('assert');
const async = require('async');
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

const bucket = `getaws${genUniqID()}`;
const memObject = `memobject-${genUniqID()}`;
const fileObject = `fileobject-${genUniqID()}`;
const awsObject = `awsobject-${genUniqID()}`;
const emptyObject = `emptyObject-${genUniqID()}`;
const emptyAwsObject = `emptyObject-${genUniqID()}`;
const bigObject = `bigObject-${genUniqID()}`;
const mismatchObject = `mismatch-${genUniqID()}`;
const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const bigBodyLen = bigBody.length;
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const emptyMD5 = 'd41d8cd98f00b204e9800998ecf8427e';
const bigMD5 = 'f1c9645dbc14efddc7d8a322685f26eb';

describe('Multiple backend get object', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        beforeAll(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterAll(() => {
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

        test(
            'should return an error to get request without a valid bucket name',
            done => {
                s3.getObject({ Bucket: '', Key: 'somekey' }, err => {
                    expect(err).not.toEqual(null);
                    expect(err.code).toBe('MethodNotAllowed');
                    done();
                });
            }
        );
        test('should return NoSuchKey error when no such object', done => {
            s3.getObject({ Bucket: bucket, Key: 'nope' }, err => {
                expect(err).not.toEqual(null);
                expect(err.code).toBe('NoSuchKey');
                done();
            });
        });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' +
        'location with bucketMatch: true ', () => {
            beforeEach(done => {
                testContext.currentTest.key = `somekey-${genUniqID()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                async.waterfall([
                    next => s3.createMultipartUpload({
                        Bucket: bucket, Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint': awsLocation,
                    } }, (err, res) => next(err, res.UploadId)),
                    (uploadId, next) => s3.uploadPart({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'helloworld' }, (err, res) => next(err, uploadId,
                        res.ETag)),
                    (uploadId, eTag, next) => s3.completeMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        MultipartUpload: {
                            Parts: [
                                {
                                    ETag: eTag,
                                    PartNumber: 1,
                                },
                            ],
                        },
                        UploadId: uploadId,
                    }, err => next(err)),
                ], done);
            });
            test('should get object from MPU on AWS ' +
            'location with bucketMatch: true ', done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: testContext.test.key,
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ContentLength).toBe('10');
                    expect(res.Body.toString()).toBe('helloworld');
                    assert.deepStrictEqual(res.Metadata,
                      { 'scal-location-constraint': awsLocation });
                    return done(err);
                });
            });
        });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' +
        'location with bucketMatch: false ', () => {
            beforeEach(done => {
                testContext.currentTest.key = `somekey-${genUniqID()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                async.waterfall([
                    next => s3.createMultipartUpload({
                        Bucket: bucket, Key: testContext.currentTest.key,
                        Metadata: { 'scal-location-constraint':
                        awsLocationMismatch,
                    } }, (err, res) => next(err, res.UploadId)),
                    (uploadId, next) => s3.uploadPart({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        PartNumber: 1,
                        UploadId: uploadId,
                        Body: 'helloworld' }, (err, res) => next(err, uploadId,
                        res.ETag)),
                    (uploadId, eTag, next) => s3.completeMultipartUpload({
                        Bucket: bucket,
                        Key: testContext.currentTest.key,
                        MultipartUpload: {
                            Parts: [
                                {
                                    ETag: eTag,
                                    PartNumber: 1,
                                },
                            ],
                        },
                        UploadId: uploadId,
                    }, err => next(err)),
                ], done);
            });
            test('should get object from MPU on AWS ' +
            'location with bucketMatch: false ', done => {
                s3.getObject({
                    Bucket: bucket,
                    Key: testContext.test.key,
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ContentLength).toBe('10');
                    expect(res.Body.toString()).toBe('helloworld');
                    assert.deepStrictEqual(res.Metadata,
                      { 'scal-location-constraint': awsLocationMismatch });
                    return done(err);
                });
            });
        });

        describeSkipIfNotMultiple('with objects in all available backends ' +
            '(mem/file/AWS)', () => {
            beforeAll(() => {
                process.stdout.write('Putting object to mem\n');
                return s3.putObjectAsync({ Bucket: bucket, Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation } })
                .then(() => {
                    process.stdout.write('Putting object to file\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: fileObject,
                        Body: body,
                        Metadata:
                        { 'scal-location-constraint': fileLocation },
                    });
                })
                .then(() => {
                    process.stdout.write('Putting object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: awsObject,
                        Body: body,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to mem\n');
                    return s3.putObjectAsync({ Bucket: bucket, Key: emptyObject,
                        Metadata:
                        { 'scal-location-constraint': memLocation },
                    });
                })
                .then(() => {
                    process.stdout.write('Putting 0-byte object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: emptyAwsObject,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .then(() => {
                    process.stdout.write('Putting large object to AWS\n');
                    return s3.putObjectAsync({ Bucket: bucket,
                        Key: bigObject, Body: bigBody,
                        Metadata: {
                            'scal-location-constraint': awsLocation } });
                })
                .catch(err => {
                    process.stdout.write(`Error putting objects: ${err}\n`);
                    throw err;
                });
            });
            test('should get an object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: memObject }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ETag).toBe(`"${correctMD5}"`);
                    done();
                });
            });
            test('should get a 0-byte object from mem', done => {
                s3.getObject({ Bucket: bucket, Key: emptyObject },
                (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ETag).toBe(`"${emptyMD5}"`);
                    done();
                });
            });
            test('should get a 0-byte object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: emptyAwsObject },
                (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ETag).toBe(`"${emptyMD5}"`);
                    done();
                });
            });
            test('should get an object from file', done => {
                s3.getObject({ Bucket: bucket, Key: fileObject },
                    (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
            });
            test('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: awsObject },
                    (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${correctMD5}"`);
                        done();
                    });
            });
            test('should get a large object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: bigObject },
                    (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ETag).toBe(`"${bigMD5}"`);
                        done();
                    });
            });
            test('should get an object using range query from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: bigObject,
                    Range: 'bytes=0-9' },
                    (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.ContentLength).toBe('10');
                        expect(res.ContentRange).toBe(`bytes 0-9/${bigBodyLen}`);
                        expect(res.ETag).toBe(`"${bigMD5}"`);
                        done();
                    });
            });
        });

        describeSkipIfNotMultiple('with bucketMatch set to false', () => {
            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: mismatchObject, Body: body,
                Metadata: { 'scal-location-constraint': awsLocationMismatch } },
                err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should get an object from AWS', done => {
                s3.getObject({ Bucket: bucket, Key: mismatchObject },
                (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.ETag).toBe(`"${correctMD5}"`);
                    done();
                });
            });
        });
    });
});
