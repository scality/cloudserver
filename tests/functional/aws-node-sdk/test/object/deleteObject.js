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
    HeadObjectCommand
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
            const bucketName = `testdeletempu-${Date.now()}`;
            before(async () => {
                try {
                    process.stdout.write('creating bucket\n');
                    await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                    
                    process.stdout.write('initiating multipart upload\n');
                    const createRes = await s3.send(new CreateMultipartUploadCommand({
                        Bucket: bucketName,
                        Key: objectName,
                    }));
                    
                    process.stdout.write('uploading parts\n');
                    uploadId = createRes.UploadId;
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

            it('should delete a object uploaded in parts successfully', done => {
                s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: objectName }))
                    .then(() => {
                        done();
                    })
                    .catch(err => {
                        assert.fail(`Expected success, got error ${JSON.stringify(err)}`);
                    });
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
                    
                    process.stdout.write('putting object\n');
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
                await bucketUtil.empty(bucketName, true);
                await bucketUtil.deleteOne(bucketName);
            });

            it('should put delete marker if no version id specified', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }))
                    .then(() => {
                        done();
                    })
                    .catch(err => {
                        assert.ifError(err);
                        done();
                    });
            });

            it('should not delete object version locked with object retention', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionIdOne,
                }))
                    .then(() => {
                        assert.fail('Should have failed');
                    })
                    .catch(err => {
                        assert.strictEqual(err.name, 'AccessDenied');
                        done();
                    });
            });

            it('should delete locked object version with GOVERNANCE retention mode and correct header', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionIdOne,
                    BypassGovernanceRetention: true,
                }))
                    .then(() => {
                        done();
                    })
                    .catch(err => {
                        assert.ifError(err);
                        done();
                    });
            });

            it('should not delete object locked with legal hold', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectNameTwo,
                    VersionId: versionIdTwo,
                }))
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

           it('should not delete locked object version with GOVERNANCE ' +
                'retention mode and bypass header when object is legal-hold enabled', done => {
                s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    VersionId: versionId,
                    BypassGovernanceRetention: true,
                }))
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

        describe('with conditional headers (unofficial, for backbeat)', () => {
            const bucketName = 'testconditionaldelete';
            const testObjectKey = 'conditional-test-object';
            const testObjectBody = 'body';
            let objectLastModified;

            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            });

            beforeEach(async () => {
                // Re-create the object for each test since some tests will delete it
                await s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: testObjectKey,
                    Body: testObjectBody,
                }));
                const head = await s3.send(new HeadObjectCommand({
                    Bucket: bucketName,
                    Key: testObjectKey,
                }));
                objectLastModified = head.LastModified;
            });

            after(async () => {
                await bucketUtil.empty(bucketName, true);
                await bucketUtil.deleteOne(bucketName);
            });

            function deleteObjectConditional(s3, params, headers, next) {
                const command = new DeleteObjectCommand(params);
                // Create a unique middleware name to avoid conflicts
                const middlewareName = `headersAdder_${Date.now()}_${Math.random()}`;
                
                // Middleware to add custom headers
                const middleware = next => async args => {
                    for (const [key, value] of Object.entries(headers)) {
                        // Ensure all header values are strings
                        // eslint-disable-next-line no-param-reassign
                        args.request.headers[key] = String(value);
                    }
                    return next(args);
                };
                
                const middlewareConfig = {
                    step: 'build',
                    name: middlewareName,
                };
                
                // Add middleware
                s3.middlewareStack.add(middleware, middlewareConfig);
                
                s3.send(command)
                    .then(data => {
                        s3.middlewareStack.remove(middlewareName);
                        next(null, data);
                    })
                    .catch(err => {
                        s3.middlewareStack.remove(middlewareName);
                        next(err);
                    });
            }

            describe('If-Unmodified-Since header tests', () => {
                it('should delete when condition is true (date after object modification)', done => {
                    const futureDate = new Date(objectLastModified.getTime() + 60_000); // 1 minute later

                    deleteObjectConditional(s3, {
                        Bucket: bucketName,
                        Key: testObjectKey,
                    }, {
                        'If-Unmodified-Since': futureDate.toUTCString(),
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(data.$metadata.httpStatusCode, 204);
                        s3.send(new HeadObjectCommand({
                            Bucket: bucketName,
                            Key: testObjectKey,
                        }))
                            .then(() => {
                                assert.fail('Object should not exist');
                            })
                            .catch(err => {
                                assert.strictEqual(err.name, 'NotFound');
                                done();
                            });
                    });
                });

                it('should fail (412) to delete when condition is false (date before object modification)', done => {
                    const pastDate = new Date(objectLastModified.getTime() - 60_000); // 1 minute earlier

                    deleteObjectConditional(s3, {
                        Bucket: bucketName,
                        Key: testObjectKey,
                    }, {
                        'If-Unmodified-Since': pastDate.toUTCString(),
                    }, err => {
                        assert.strictEqual(err.name, 'PreconditionFailed');
                        assert.strictEqual(err.$metadata.httpStatusCode, 412);
                        done();
                    });
                });
            });

            describe('If-Modified-Since header tests', () => {
                it('should delete when condition is true (date before object modification)', done => {
                    const pastDate = new Date(objectLastModified.getTime() - 60_000); // 1 minute earlier

                    deleteObjectConditional(s3, {
                        Bucket: bucketName,
                        Key: testObjectKey,
                    }, {
                        'If-Modified-Since': pastDate.toUTCString()
                    }, (err, data) => {
                        assert.deepStrictEqual(data.$metadata.httpStatusCode, 204);
                        s3.send(new HeadObjectCommand({
                            Bucket: bucketName,
                            Key: testObjectKey,
                        }))
                        .catch(err => {
                            assert.strictEqual(err.name, 'NotFound');
                            done();
                        });
                    });
                });

                it('should fail (304) to delete when condition is false (date after object modification)', done => {
                    const futureDate = new Date(objectLastModified.getTime() + 60_000); // 1 minute later

                    deleteObjectConditional(s3, {
                        Bucket: bucketName,
                        Key: testObjectKey,
                    }, {
                        'If-Modified-Since': futureDate.toUTCString(),
                    }, err => {
                        assert.strictEqual(err.$metadata.httpStatusCode, 304);
                        done();
                    });
                });
            });

            describe('combined conditional headers', () => {
                it('should delete when both If-Modified-Since and If-Unmodified-Since conditions are true', done => {
                    const pastDate = new Date(objectLastModified.getTime() - 60_000); // 1 minute earlier
                    const futureDate = new Date(objectLastModified.getTime() + 60_000); // 1 minute later

                    deleteObjectConditional(s3, {
                        Bucket: bucketName,
                        Key: testObjectKey,
                    }, {
                        'If-Modified-Since': pastDate.toUTCString(),
                        'If-Unmodified-Since': futureDate.toUTCString(),
                    }, (err, data) => {
                        assert.deepStrictEqual(data.$metadata.httpStatusCode, 204);
                        s3.send(new HeadObjectCommand({
                            Bucket: bucketName,
                            Key: testObjectKey,
                        }))
                        .catch(err => {
                            assert.strictEqual(err.name, 'NotFound');
                            assert.strictEqual(err.$metadata.httpStatusCode, 404);
                            done();
                        });
                    });
                });
            });
        });
    });
});
