const assert = require('assert');
const moment = require('moment');
const Promise = require('bluebird');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const changeObjectLock = require('../../../../utilities/objectLock-util');

const bucketName = 'testdeletempu';
const objectName = 'key';
const objectNameTwo = 'secondkey';

const isCEPH = process.env.CI_CEPH !== undefined;
const describeSkipIfCeph = isCEPH ? describe.skip : describe;

describe('DELETE object', () => {
    withV4(sigCfg => {
        let uploadId;
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const testfile = Buffer.alloc(1024 * 1024 * 54, 0);

        describe('with multipart upload', () => {
            before(() => {
                process.stdout.write('creating bucket\n');
                return s3.createBucket({ Bucket: bucketName }).promise()
                .then(() => {
                    process.stdout.write('initiating multipart upload\n');
                    return s3.createMultipartUpload({
                        Bucket: bucketName,
                        Key: objectName,
                    }).promise();
                })
                .then(res => {
                    process.stdout.write('uploading parts\n');
                    uploadId = res.UploadId;
                    const uploads = [];
                    for (let i = 1; i <= 3; i++) {
                        uploads.push(
                            s3.uploadPart({
                                Bucket: bucketName,
                                Key: objectName,
                                PartNumber: i,
                                Body: testfile,
                                UploadId: uploadId,
                            }).promise()
                        );
                    }
                    return Promise.all(uploads);
                })
                .catch(err => {
                    process.stdout.write(`Error with uploadPart ${err}\n`);
                    throw err;
                })
                .then(res => {
                    process.stdout.write('about to complete multipart ' +
                        'upload\n');
                    return s3.completeMultipartUpload({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: res[0].ETag, PartNumber: 1 },
                                { ETag: res[1].ETag, PartNumber: 2 },
                                { ETag: res[2].ETag, PartNumber: 3 },
                            ],
                        },
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write('completeMultipartUpload error: ' +
                        `${err}\n`);
                    throw err;
                });
            });

            after(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil.empty(bucketName)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(bucketName);
                })
                .catch(err => {
                    process.stdout.write('Error in after\n');
                    throw err;
                });
            });

            it('should delete a object uploaded in parts successfully',
            done => {
                s3.deleteObject({ Bucket: bucketName, Key: objectName },
                err => {
                    assert.strictEqual(err, null,
                        `Expected success, got error ${JSON.stringify(err)}`);
                    done();
                });
            });
        });

        describeSkipIfCeph('with object lock', () => {
            let versionIdOne;
            let versionIdTwo;
            const retainDate = moment().add(10, 'days').toISOString();
            before(() => {
                process.stdout.write('creating bucket\n');
                return s3.createBucket({
                    Bucket: bucketName,
                    ObjectLockEnabledForBucket: true,
                }).promise()
                .catch(err => {
                    process.stdout.write(`Error creating bucket ${err}\n`);
                    throw err;
                })
                .then(() => {
                    process.stdout.write('putting object\n');
                    return s3.putObject({
                        Bucket: bucketName,
                        Key: objectName,
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write('Error putting object');
                    throw err;
                })
                .then(res => {
                    versionIdOne = res.VersionId;
                    process.stdout.write('putting object retention\n');
                    return s3.putObjectRetention({
                        Bucket: bucketName,
                        Key: objectName,
                        Retention: {
                            Mode: 'GOVERNANCE',
                            RetainUntilDate: retainDate,
                        },
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write('Err putting object retention\n');
                    throw err;
                })
                .then(() => {
                    process.stdout.write('putting object\n');
                    return s3.putObject({
                        Bucket: bucketName,
                        Key: objectNameTwo,
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write(('Err putting second object\n'));
                    throw err;
                })
                .then(res => {
                    versionIdTwo = res.VersionId;
                    process.stdout.write('putting object legal hold\n');
                    return s3.putObjectLegalHold({
                        Bucket: bucketName,
                        Key: objectNameTwo,
                        LegalHold: {
                            Status: 'ON',
                        },
                    }).promise();
                })
                .catch(err => {
                    process.stdout.write('Err putting object legal hold\n');
                    throw err;
                });
            });

            after(() => {
                process.stdout.write('Emptying bucket\n');
                return bucketUtil.empty(bucketName)
                .then(() => {
                    process.stdout.write('Deleting bucket\n');
                    return bucketUtil.deleteOne(bucketName);
                })
                .catch(err => {
                    process.stdout.write('Error in after\n');
                    throw err;
                });
            });

            it('should put delete marker if no version id specified', done => {
                s3.deleteObject({
                    Bucket: bucketName,
                    Key: objectName,
                }, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should not delete object version locked with object ' +
            'retention', done => {
                s3.deleteObject({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionIdOne,
                }, err => {
                    assert.strictEqual(err.code, 'AccessDenied');
                    done();
                });
            });

            it('should delete locked object version with GOVERNANCE ' +
            'retention mode and correct header', done => {
                s3.deleteObject({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionIdOne,
                    BypassGovernanceRetention: true,
                }, err => {
                    assert.ifError(err);
                    done();
                });
            });

            it('should not delete object locked with legal hold', done => {
                s3.deleteObject({
                    Bucket: bucketName,
                    Key: objectNameTwo,
                    VersionId: versionIdTwo,
                }, err => {
                    assert.strictEqual(err.code, 'AccessDenied');
                    changeObjectLock(
                        [{
                            bucket: bucketName,
                            key: objectNameTwo,
                            versionId: versionIdTwo,
                        }], '', done);
                });
            });
        });
    });
});
