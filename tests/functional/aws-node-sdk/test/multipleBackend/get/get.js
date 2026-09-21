const assert = require('assert');
const {
    CreateBucketCommand,
    GetObjectCommand,
    PutObjectCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
} = require('@aws-sdk/client-s3');
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

describe('Multiple backend get object', function testSuite() {
    this.timeout(30000);
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        before(() => {
            process.stdout.write('Creating bucket');
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const command = new CreateBucketCommand({ Bucket: bucket });
            return s3.send(command).catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil
                .empty(bucket)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(bucket);
                })
                .catch(err => {
                    process.stdout.write('Error emptying/deleting bucket: ' + `${err}\n`);
                    throw err;
                });
        });

        // aws-sdk now (v2.363.0) returns 'UriParameterError' error
        it.skip('should return an error to get request without a valid ' + 'bucket name', done => {
            const command = new GetObjectCommand({ Bucket: '', Key: 'somekey' });
            s3.send(command)
                .then(() => done(new Error('Expected failure but got success')))
                .catch(err => {
                    assert.notEqual(err, null, 'Expected failure but got success');
                    assert.strictEqual(err.name, 'MethodNotAllowed');
                    done();
                });
        });
        it('should return NoSuchKey error when no such object', done => {
            const command = new GetObjectCommand({ Bucket: bucket, Key: 'nope' });
            s3.send(command)
                .then(() => done(new Error('Expected failure but got success')))
                .catch(err => {
                    assert.notEqual(err, null, 'Expected failure but got success');
                    assert.strictEqual(err.name, 'NoSuchKey');
                    done();
                });
        });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' + 'location with bucketMatch: true ', () => {
            beforeEach(function beforeEachFn() {
                this.currentTest.key = `somekey-${genUniqID()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                return s3
                    .send(
                        new CreateMultipartUploadCommand({
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            Metadata: { 'scal-location-constraint': awsLocation },
                        }),
                    )
                    .then(res => {
                        const uploadId = res.UploadId;
                        const partBody = Buffer.from('helloworld', 'utf8');
                        const uploadPartInput = {
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: partBody,
                            ContentLength: partBody.length,
                        };
                        const uploadPartCommand = new UploadPartCommand(uploadPartInput);
                        uploadPartCommand.middlewareStack.add(
                            next => async args => {
                                const headers = args.request?.headers;
                                if (headers) {
                                    headers['Content-Length'] = `${partBody.length}`;
                                    headers['x-amz-decoded-content-length'] = `${partBody.length}`;
                                }
                                return next(args);
                            },
                            { step: 'build' },
                        );
                        return s3.send(uploadPartCommand).then(partRes => {
                            const eTag = partRes.ETag;
                            return s3.send(
                                new CompleteMultipartUploadCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.key,
                                    MultipartUpload: {
                                        Parts: [
                                            {
                                                ETag: eTag,
                                                PartNumber: 1,
                                            },
                                        ],
                                    },
                                    UploadId: uploadId,
                                }),
                            );
                        });
                    })
                    .catch(err => {
                        process.stdout.write(`Error in beforeEach: ${err}\n`);
                        throw err;
                    });
            });
            it('should get object from MPU on AWS ' + 'location with bucketMatch: true ', function it(done) {
                const command = new GetObjectCommand({
                    Bucket: bucket,
                    Key: this.test.key,
                });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ContentLength, 10);
                        assert.strictEqual(res.Body.toString(), 'helloworld');
                        assert.deepStrictEqual(res.Metadata, { 'scal-location-constraint': awsLocation });
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
        });

        describeSkipIfNotMultiple('Complete MPU then get object on AWS ' + 'location with bucketMatch: false ', () => {
            beforeEach(function beforeEachFn() {
                this.currentTest.key = `somekey-${genUniqID()}`;
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;

                return s3
                    .send(
                        new CreateMultipartUploadCommand({
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            Metadata: { 'scal-location-constraint': awsLocationMismatch },
                        }),
                    )
                    .then(res => {
                        const uploadId = res.UploadId;
                        const partBody = Buffer.from('helloworld', 'utf8');
                        const uploadPartInput = {
                            Bucket: bucket,
                            Key: this.currentTest.key,
                            PartNumber: 1,
                            UploadId: uploadId,
                            Body: partBody,
                            ContentLength: partBody.length,
                        };
                        const uploadPartCommand = new UploadPartCommand(uploadPartInput);
                        uploadPartCommand.middlewareStack.add(
                            next => async args => {
                                const headers = args.request?.headers;
                                if (headers) {
                                    headers['content-length'] = `${partBody.length}`;
                                    headers['x-amz-decoded-content-length'] = `${partBody.length}`;
                                }
                                return next(args);
                            },
                            { step: 'build' },
                        );
                        return s3.send(uploadPartCommand).then(partRes => {
                            const eTag = partRes.ETag;
                            return s3.send(
                                new CompleteMultipartUploadCommand({
                                    Bucket: bucket,
                                    Key: this.currentTest.key,
                                    MultipartUpload: {
                                        Parts: [
                                            {
                                                ETag: eTag,
                                                PartNumber: 1,
                                            },
                                        ],
                                    },
                                    UploadId: uploadId,
                                }),
                            );
                        });
                    })
                    .catch(err => {
                        process.stdout.write(`Error in beforeEach: ${err}\n`);
                        throw err;
                    });
            });
            it('should get object from MPU on AWS ' + 'location with bucketMatch: false ', function it(done) {
                const command = new GetObjectCommand({
                    Bucket: bucket,
                    Key: this.test.key,
                });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ContentLength, 10);
                        assert.strictEqual(res.Body.toString(), 'helloworld');
                        assert.deepStrictEqual(res.Metadata, { 'scal-location-constraint': awsLocationMismatch });
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
        });

        describeSkipIfNotMultiple('with objects in all available backends ' + '(mem/file/AWS)', () => {
            before(() => {
                process.stdout.write('Putting object to mem\n');
                const memCommand = new PutObjectCommand({
                    Bucket: bucket,
                    Key: memObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': memLocation },
                });
                return s3
                    .send(memCommand)
                    .then(() => {
                        process.stdout.write('Putting object to file\n');
                        const fileCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: fileObject,
                            Body: body,
                            Metadata: { 'scal-location-constraint': fileLocation },
                        });
                        return s3.send(fileCommand);
                    })
                    .then(() => {
                        process.stdout.write('Putting object to AWS\n');
                        const awsCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: awsObject,
                            Body: body,
                            Metadata: { 'scal-location-constraint': awsLocation },
                        });
                        return s3.send(awsCommand);
                    })
                    .then(() => {
                        process.stdout.write('Putting 0-byte object to mem\n');
                        const emptyCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: emptyObject,
                            Metadata: { 'scal-location-constraint': memLocation },
                        });
                        return s3.send(emptyCommand);
                    })
                    .then(() => {
                        process.stdout.write('Putting 0-byte object to AWS\n');
                        const emptyAwsCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: emptyAwsObject,
                            Metadata: { 'scal-location-constraint': awsLocation },
                        });
                        return s3.send(emptyAwsCommand);
                    })
                    .then(() => {
                        process.stdout.write('Putting large object to AWS\n');
                        const bigCommand = new PutObjectCommand({
                            Bucket: bucket,
                            Key: bigObject,
                            Body: bigBody,
                            Metadata: { 'scal-location-constraint': awsLocation },
                        });
                        return s3.send(bigCommand);
                    })
                    .catch(err => {
                        process.stdout.write(`Error putting objects: ${err}\n`);
                        throw err;
                    });
            });
            it('should get an object from mem', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: memObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get a 0-byte object from mem', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: emptyObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get a 0-byte object from AWS', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: emptyAwsObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${emptyMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got error ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get an object from file', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: fileObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get an object from AWS', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: awsObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get a large object from AWS', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: bigObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
            it('should get an object using range query from AWS', done => {
                const command = new GetObjectCommand({
                    Bucket: bucket,
                    Key: bigObject,
                    Range: 'bytes=0-9',
                });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ContentLength, 10);
                        assert.strictEqual(res.ContentRange, `bytes 0-9/${bigBodyLen}`);
                        assert.strictEqual(res.ETag, `"${bigMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, 'Expected success but got ' + `error ${err}`);
                        done(err);
                    });
            });
        });
        describeSkipIfNotMultiple('with bucketMatch set to false', () => {
            beforeEach(done => {
                const command = new PutObjectCommand({
                    Bucket: bucket,
                    Key: mismatchObject,
                    Body: body,
                    Metadata: { 'scal-location-constraint': awsLocationMismatch },
                });
                s3.send(command)
                    .then(() => done())
                    .catch(err => {
                        assert.equal(err, null, `Err putting object: ${err}`);
                        done(err);
                    });
            });

            it('should get an object from AWS', done => {
                const command = new GetObjectCommand({ Bucket: bucket, Key: mismatchObject });
                s3.send(command)
                    .then(res => {
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    })
                    .catch(err => {
                        assert.equal(err, null, `Error getting object: ${err}`);
                        done(err);
                    });
            });
        });
    });
});
