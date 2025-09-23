const assert = require('assert');
const moment = require('moment');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    CompleteMultipartUploadCommand,
    DeleteObjectCommand,
    PutObjectCommand,
    PutObjectRetentionCommand,
    PutObjectLegalHoldCommand,
    PutObjectLockConfigurationCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const changeObjectLock = require('../../../../utilities/objectLock-util');

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
            const bucketName = 'testdeletempu';
            before(async () => {
                try {
                    process.stdout.write('creating bucket\n');
                    await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                    
                    process.stdout.write('initiating multipart upload\n');
                    const res = await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                    }));
                    
                    uploadId = res.UploadId;
                    process.stdout.write('uploading parts\n');
                    const uploads = [];
                    for (let i = 1; i <= 3; i++) {
                        uploads.push(
                            s3.send(new UploadPartCommand({
                                Bucket: bucketName,
                                Key: objectName,
                                PartNumber: i,
                                Body: testfile,
                                UploadId: uploadId,
                            }))
                        );
                    }
                    const uploadResults = await Promise.all(uploads);
                    
                    process.stdout.write('about to complete multipart upload\n');
                    await s3.send(new CompleteMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        UploadId: uploadId,
                        MultipartUpload: {
                            Parts: [
                                { ETag: uploadResults[0].ETag, PartNumber: 1 },
                                { ETag: uploadResults[1].ETag, PartNumber: 2 },
                                { ETag: uploadResults[2].ETag, PartNumber: 3 },
                            ],
                        },
                    }));
                } catch (err) {
                    process.stdout.write(`Error in before: ${err}\n`);
                    throw err;
                }
            });

            after(async () => {
                try {
                    process.stdout.write('Emptying bucket\n');
                    await bucketUtil.empty(bucketName);
                    process.stdout.write('Deleting bucket\n');
                    await bucketUtil.deleteOne(bucketName);
                } catch (err) {
                    process.stdout.write('Error in after\n');
                    throw err;
                }
            });

            it('should delete a object uploaded in parts successfully', async () => {
                await s3.send(new DeleteObjectCommand({ 
                    Bucket: bucketName, 
                    Key: objectName 
                }));
            });
        });

        describeSkipIfCeph('with object lock', () => {
            const bucketName = 'testdeleteobjectlockbucket';
            let versionIdOne;
            let versionIdTwo;
            const retainDate = moment().add(10, 'days').toDate();
            before(async () => {
                try {
                    process.stdout.write('creating bucket\n');
                    await s3.send(new CreateBucketCommand({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }));
                    
                    process.stdout.write('putting object\n');
                    const res1 = await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                    }));
                    versionIdOne = res1.VersionId;
                    
                    process.stdout.write('putting object retention\n');
                    await s3.send(new PutObjectRetentionCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Retention: {
                            Mode: 'GOVERNANCE',
                            RetainUntilDate: retainDate,
                        },
                    }));
                    
                    process.stdout.write('putting second object\n');
                    const res2 = await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: objectNameTwo,
                    }));
                    versionIdTwo = res2.VersionId;
                    
                    process.stdout.write('putting object legal hold\n');
                    await s3.send(new PutObjectLegalHoldCommand({
                        Bucket: bucketName,
                        Key: objectNameTwo,
                        LegalHold: {
                            Status: 'ON',
                        },
                    }));
                } catch (err) {
                    process.stdout.write(`Error in before: ${err}\n`);
                    throw err;
                }
            });

            after(async () => {
                try {
                    process.stdout.write('Emptying bucket\n');
                    await bucketUtil.empty(bucketName);
                    process.stdout.write('Deleting bucket\n');
                    await bucketUtil.deleteOne(bucketName);
                } catch (err) {
                    process.stdout.write('Error in after\n');
                    throw err;
                }
            });

            it('should put delete marker if no version id specified', async () => {
                await s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }));
            });

            it('should not delete object version locked with object retention', async () => {
                try {
                    await s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        VersionId: versionIdOne,
                    }));
                    assert.fail('Expected AccessDenied error');
                } catch (err) {
                    assert.strictEqual(err.name, 'AccessDenied');
                }
            });

            it('should delete locked object version with GOVERNANCE retention mode and correct header', async () => {
                await s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionIdOne,
                    BypassGovernanceRetention: true,
                }));
            });

            it('should not delete object locked with legal hold', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectNameTwo,
                    VersionId: versionIdTwo,
                }))
                .then(() => {
                    done(new Error('Expected AccessDenied error'));
                })
                .catch(err => {
                    assert.strictEqual(err.name, 'AccessDenied');
                    changeObjectLock(
                        [{
                            bucket: bucketName,
                            key: objectNameTwo,
                            versionId: versionIdTwo,
                        }], '', done);
                });
            });
        });

        describeSkipIfCeph('with object lock and legal hold', () => {
            const bucketName = 'testdeletelocklegalholdbucket';
            const objectName = 'key';
            let versionId;
            
            before(async () => {
                try {
                    process.stdout.write('creating bucket\n');
                    await s3.send(new CreateBucketCommand({
                        Bucket: bucketName,
                        ObjectLockEnabledForBucket: true,
                    }));
                    
                    process.stdout.write('putting object lock configuration\n');
                    await s3.send(new PutObjectLockConfigurationCommand({
                        Bucket: bucketName,
                        ObjectLockConfiguration: {
                            ObjectLockEnabled: 'Enabled',
                            Rule: {
                                DefaultRetention: {
                                    Mode: 'GOVERNANCE',
                                    Days: 1,
                                },
                            },
                        },
                    }));
                    
                    process.stdout.write('putting object\n');
                    const res = await s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                    }));
                    versionId = res.VersionId;
                    
                    process.stdout.write('putting object legal hold\n');
                    await s3.send(new PutObjectLegalHoldCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        LegalHold: {
                            Status: 'ON',
                        },
                    }));
                } catch (err) {
                    process.stdout.write(`Error in before: ${err}\n`);
                    throw err;
                }
            });

            after(async () => {
                try {
                    process.stdout.write('Emptying bucket\n');
                    await bucketUtil.empty(bucketName);
                    process.stdout.write('Deleting bucket\n');
                    await bucketUtil.deleteOne(bucketName);
                } catch (err) {
                    process.stdout.write('Error in after\n');
                    throw err;
                }
            });

            it('should not delete locked object version with GOVERNANCE retention '+
                'mode and bypass header when object is legal-hold enabled', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    BypassGovernanceRetention: true,
                }))
                .then(() => {
                    done(new Error('Expected AccessDenied error'));
                })
                .catch(err => {
                    assert.strictEqual(err.name, 'AccessDenied');
                    changeObjectLock(
                        [{
                            bucket: bucketName,
                            key: objectName,
                            versionId,
                        }], '', done);
                });
            });
        });
    });
});
