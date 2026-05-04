const assert = require('assert');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const changeObjectLock = require('../../../../utilities/objectLock-util');
const { fakeMetadataTransition, fakeMetadataArchive } = require('../utils/init');
const {
    CopyObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    GetObjectTaggingCommand,
    PutObjectCommand,
    GetObjectAclCommand,
    PutObjectAclCommand
} = require('@aws-sdk/client-s3');

const { taggingTests } = require('../../lib/utility/tagging');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');
const constants = require('../../../../../constants');

const sourceBucketName = 'supersourcebucket8102016';
const sourceObjName = 'supersourceobject';
const destBucketName = 'destinationbucket8102016';
const destObjName = 'copycatobject';

const originalMetadata = {
    oldmetadata: 'same old',
    overwriteme: 'wipe me out with replace',
};
const originalCacheControl = 'max-age=1337';
const originalContentDisposition = 'attachment; filename="1337.txt";';
const originalContentEncoding = 'base64,aws-chunked';
const originalExpires = new Date(12345678);

const originalTagKey = 'key1';
const originalTagValue = 'value1';
const originalTagging = `${originalTagKey}=${originalTagValue}`;

const newMetadata = {
    newmetadata: 'new kid in town',
    overwriteme: 'wiped',
};
const newCacheControl = 'max-age=86400';
const newContentDisposition = 'attachment; filename="fname.ext";';
const newContentEncoding = 'gzip,aws-chunked';
const newExpires = new Date();

const newTagKey = 'key2';
const newTagValue = 'value2';
const newTagging = `${newTagKey}=${newTagValue}`;

const content = 'I am the best content ever';

const otherAccountBucketUtility = new BucketUtility('lisa', {});
const otherAccountS3 = otherAccountBucketUtility.s3;

const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d;
}

function dateConvert(d) {
    return new Date(d);
}


describe('Object Copy', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let etag;
        let etagTrim;
        let lastModified;


        before(async () => {
            try {
                bucketUtil = new BucketUtility('default', sigCfg);
                s3 = bucketUtil.s3;
                await bucketUtil.empty(sourceBucketName);
                await bucketUtil.empty(destBucketName);
                await bucketUtil.deleteMany([sourceBucketName, destBucketName]);
            } catch (err) {
                if (err.name !== 'NoSuchBucket') {
                    process.stdout.write(`${err}\n`);
                    throw err;
                }
            }
            await bucketUtil.createOne(sourceBucketName);
            await bucketUtil.createOne(destBucketName);
        });

        beforeEach(() => s3.send(new PutObjectCommand({
            Bucket: sourceBucketName,
            Key: sourceObjName,
            Body: content,
            Metadata: originalMetadata,
            CacheControl: originalCacheControl,
            ContentDisposition: originalContentDisposition,
            ContentEncoding: originalContentEncoding,
            Expires: originalExpires,
            Tagging: originalTagging,
        })).then(res => {
            etag = res.ETag;
            etagTrim = etag.substring(1, etag.length - 1);
            return s3.send(new HeadObjectCommand({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            }));
        }).then(res => {
            lastModified = res.LastModified;
        }));

        afterEach(async () => {
            await bucketUtil.empty(sourceBucketName, true);
            await bucketUtil.empty(destBucketName, true);
        });

        after(async () => await bucketUtil.deleteMany([sourceBucketName, destBucketName]));

        function requestCopy(fields, cb) {
            s3.send(new CopyObjectCommand(Object.assign({
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
            }, fields))).then(res => {
                cb(null, res);
            }).catch(cb);
        }

        async function successCopyCheck(error, response, copyVersionMetadata, destBucketName, destObjName) {
            checkNoError(error);
            assert.strictEqual(response.ETag, etag);
            const copyLastModified = new Date(response.LastModified).toGMTString();
            
            const res = await s3.send(new GetObjectCommand({ 
                Bucket: destBucketName,
                Key: destObjName 
            }));
            assert.strictEqual(res.StorageClass, undefined);
            const bodyString = await res.Body.transformToString();
            assert.strictEqual(bodyString, content);
            assert.deepStrictEqual(res.Metadata, copyVersionMetadata);
            assert.strictEqual(res.LastModified.toGMTString(), copyLastModified);
        }

        function checkSuccessTagging(key, value, cb) {
            s3.send(new GetObjectTaggingCommand({ Bucket: destBucketName, Key: destObjName })).then(data => {
                assert.strictEqual(data.TagSet[0].Key, key);
                assert.strictEqual(data.TagSet[0].Value, value);
                cb();
            }).catch(err => {
                checkNoError(err);
                cb(err);
            });
        }

        function checkNoTagging(cb) {
            s3.send(new GetObjectTaggingCommand({ Bucket: destBucketName, Key: destObjName })).then(data => {
                assert.strictEqual(data.TagSet.length, 0);
                cb();
            }).catch(err => {
                checkNoError(err);
                cb(err);
            });
        }

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the metadata if no metadata directive ' +
            'header provided', async () => {
            const res = await s3.send(new CopyObjectCommand({ 
                Bucket: destBucketName, 
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` 
            }));
            await successCopyCheck(null, res.CopyObjectResult, originalMetadata,
                destBucketName, destObjName);
        });

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if no tagging directive' +
            'header provided', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` })).then(() => {
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                }).catch(err => {
                    checkNoError(err);
                });
        });

        it('should return 400 InvalidArgument if invalid tagging ' +
        'directive', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COCO' })).then(() => {
                    done(new Error('Expected 400 InvalidArgument error'));
                }).catch(err => {
                    checkError(err, 'InvalidArgument', 400);
                    done();
                });
        });

        it('should return 400 KeyTooLong if key is longer than 915 bytes', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: 'a'.repeat(916),
                CopySource: `${sourceBucketName}/${sourceObjName}` })).then(() => {
                    done(new Error('Expected 400 KeyTooLong error'));
                }).catch(err => {
                    checkError(err, 'KeyTooLong', 400);
                    done();
                });
        });

        it('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if COPY tagging ' +
            'directive header provided', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COPY' })).then(() => {
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                }).catch(err => {
                    checkNoError(err);
                });
        });

        it('should copy an object and tag set if COPY ' +
            'included as tag directive header (and ignore any new ' +
            'tag set sent with copy request)', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COPY',
                Tagging: newTagging,
            })).then(() => {
                s3.send(new GetObjectCommand({ Bucket: destBucketName,
                    Key: destObjName })).then(res => {
                    assert.deepStrictEqual(res.Metadata, originalMetadata);
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            }).catch(err => {
                checkNoError(err);
            });
        });

        it('should copy an object from a source to the same destination ' +
        'updating tag if REPLACE tagging directive header provided',
        done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE', Tagging: newTagging })).then(() => {
                    checkSuccessTagging(newTagKey, newTagValue, done);
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
        });

        it('should copy an object from a source to the same destination ' +
        'return no tag if REPLACE tagging directive header provided but ' +
        '"x-amz-tagging" header is not specified', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE' })).then(() => {
                    checkNoTagging(done);
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
        });

        it('should copy an object from a source to the same destination ' +
        'return no tag if COPY tagging directive header but provided from ' +
        'an empty object', done => {
            s3.send(new PutObjectCommand({ Bucket: sourceBucketName, Key: 'emptyobject' })).then(() => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/emptyobject`,
                    TaggingDirective: 'COPY' })).then(() => {
                    checkNoTagging(done);
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            });
        });

        it('should copy an object from a source to the same destination ' +
        'updating tag if REPLACE tagging directive header provided',
        done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE', Tagging: newTagging })).then(() => {
                    checkSuccessTagging(newTagKey, newTagValue, done);
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
        });

        describe('Copy object updating tag set', () => {
            taggingTests.forEach(taggingTest => {
                it(taggingTest.it, done => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: destBucketName, Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        TaggingDirective: 'REPLACE', Tagging: tagging };
                    s3.send(new CopyObjectCommand(params)).then(() => checkSuccessTagging(taggingTest.tag.key,
                          taggingTest.tag.value, done)).catch(err => {
                        if (taggingTest.error) {
                            checkError(err, taggingTest.error, taggingTest.code);
                            return done();
                        }
                        assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        return checkSuccessTagging(taggingTest.tag.key,
                          taggingTest.tag.value, done);
                    });
                });
            });
        });

        it('should also copy additional headers (CacheControl, ' +
            'ContentDisposition, ContentEncoding, Expires) when copying an ' +
            'object from a source bucket to a different destination bucket', done => {
              s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                  CopySource: `${sourceBucketName}/${sourceObjName}` })).then(() => {
                      s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName })).then(res => {
                          assert.strictEqual(res.CacheControl,
                              originalCacheControl);
                            assert.strictEqual(res.ContentDisposition,
                              originalContentDisposition);
                            // Should remove V4 streaming value 'aws-chunked'
                            // to be compatible with AWS behavior
                            assert.strictEqual(res.ContentEncoding,
                              'base64,'
                            );
                            assert.strictEqual(res.Expires.toGMTString(),
                                originalExpires.toGMTString());
                            done();
                        }).catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
            });

        it('should copy an object from a source bucket to a different ' +
            'key in the same bucket', async () => {
                const res = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}` }));
                await successCopyCheck(null, res.CopyObjectResult, originalMetadata,
                    sourceBucketName, destObjName);
            });

        // TODO: see S3C-3482, figure out why this test fails in Integration builds
        itSkipIfE2E('should not return error if copying object w/ > ' +
            '2KB user-defined md and COPY directive', done => {
                const metadata = genMaxSizeMetaHeaders();
                const params = {
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'COPY',
                    Metadata: metadata,
                };
                s3.send(new CopyObjectCommand(params)).then(() => {
                    // add one more byte to be over the limit
                    metadata.header0 = `${metadata.header0}${'0'}`;
                    s3.send(new CopyObjectCommand(params)).then(() => {
                        done();
                    }).catch(err => {
                        assert.strictEqual(err, null, `Unexpected err: ${err}`);
                        done(err);
                    });
                }).catch(err => {
                    assert.strictEqual(err, null, `Unexpected err: ${err}`);
                    done(err);
                });
            });

        // TODO: see S3C-3482, figure out why this test fails in Integration builds
        itSkipIfE2E('should return error if copying object w/ > 2KB ' +
            'user-defined md and REPLACE directive', async () => {
                try {
                    const metadata = genMaxSizeMetaHeaders();
                    const params = {
                        Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: metadata,
                };
                await s3.send(new CopyObjectCommand(params));
                // add one more byte to be over the limit
                metadata.header0 = `${metadata.header0}${'0'}`;
                await s3.send(new CopyObjectCommand(params));
                assert.fail('Expected MetadataTooLarge error');
            } catch (err) {
                assert.strictEqual(err.name, 'MetadataTooLarge');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
            }
        });

        it('should copy an object from a source to the same destination ' +
            '(update metadata)', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata }));
            await successCopyCheck(null, res.CopyObjectResult, newMetadata,
                sourceBucketName, sourceObjName);
        });

        it('should copy an object and replace the metadata if replace ' +
            'included as metadata directive header', async () => {
            const res = await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata }));
            await successCopyCheck(null, res.CopyObjectResult, newMetadata,
                destBucketName, destObjName);
        });

        it('should copy an object and replace ContentType if replace ' +
            'included as a metadata directive header, and new ContentType is ' +
            'provided', async () => {
            await s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                ContentType: 'image',
            }));
            const res = await s3.send(new GetObjectCommand({ Bucket: destBucketName,
                Key: destObjName }));
            assert.strictEqual(res.ContentType, 'image');
        });

        it('should copy an object and keep ContentType if replace ' +
            'included as a metadata directive header, but no new ContentType ' +
            'is provided', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
            })).then(() => {
                s3.send(new GetObjectCommand({ Bucket: destBucketName,
                    Key: destObjName })).then(res => {
                    assert.strictEqual(res.ContentType, 'application/octet-stream');
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            });
        });

        it('should also replace additional headers if replace ' +
            'included as metadata directive header and new headers are ' +
            'specified', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            })).then(() => {
                s3.send(new GetObjectCommand({ Bucket: destBucketName,
                    Key: destObjName })).then(res => {
                    assert.strictEqual(res.CacheControl, newCacheControl);
                    assert.strictEqual(res.ContentDisposition,
                      newContentDisposition);
                    // Should remove V4 streaming value 'aws-chunked'
                    // to be compatible with AWS behavior
                    assert.strictEqual(res.ContentEncoding, 'gzip,');
                    assert.strictEqual(res.Expires.toGMTString(),
                        newExpires.toGMTString());
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done(err); 
                });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        it('should copy an object and the metadata if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'metadata sent with copy request)', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
            })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: destBucketName,
                        Key: destObjName })).then(res => {
                        assert.deepStrictEqual(res.Metadata, originalMetadata);
                        done();
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        it('should copy an object and its additional headers if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'headers sent with copy request)', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            })).then(() => {
                s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName })).then(res => {
                    assert.strictEqual(res.CacheControl,
                        originalCacheControl);
                      assert.strictEqual(res.ContentDisposition,
                        originalContentDisposition);
                      assert.strictEqual(res.ContentEncoding,
                        'base64,');
                      assert.strictEqual(res.Expires.toGMTString(),
                        originalExpires.toGMTString());
                      done();
                  });
            });
        });

        it('should copy a 0 byte object to different destination', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.send(new PutObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '', Metadata: originalMetadata })).then(() => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(res => {
                        assert.strictEqual(res.CopyObjectResult.ETag, emptyFileETag);
                        s3.send(new GetObjectCommand({ Bucket: destBucketName,
                            Key: destObjName })).then(res => {
                            assert.deepStrictEqual(res.Metadata,
                                originalMetadata);
                            assert.strictEqual(res.ETag, emptyFileETag);
                            done();
                        });
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        // TODO: remove (or update to use different location constraint) in CLDSRV-639
        if (constants.validStorageClasses.includes('REDUCED_REDUNDANCY')) {
            it('should copy a 0 byte object to same destination', done => {
                const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
                s3.send(new PutObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName, Body: '' })).then(() => {
                    s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        StorageClass: 'REDUCED_REDUNDANCY',
                    })).then(res => {
                        assert.strictEqual(res.CopyObjectResult.ETag, emptyFileETag);
                        s3.send(new GetObjectCommand({ Bucket: sourceBucketName,
                            Key: sourceObjName })).then(res => {
                            assert.deepStrictEqual(res.Metadata,
                                {});
                            assert.deepStrictEqual(res.StorageClass,
                                'REDUCED_REDUNDANCY');
                            assert.strictEqual(res.ETag, emptyFileETag);
                            done();
                        }).catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
                });
            });

            it('should copy an object to a different destination and change ' +
                'the storage class if storage class header provided', done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    StorageClass: 'REDUCED_REDUNDANCY',
                })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: destBucketName,
                        Key: destObjName })).then(res => {
                        assert.strictEqual(res.StorageClass,
                            'REDUCED_REDUNDANCY');
                        done();
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            });

            it('should copy an object to the same destination and change the ' +
                'storage class if the storage class header provided', done => {
                s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    StorageClass: 'REDUCED_REDUNDANCY',
                })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: sourceBucketName,
                        Key: sourceObjName })).then(res => {
                        assert.strictEqual(res.StorageClass,
                            'REDUCED_REDUNDANCY');
                        done();
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            });
        }

        it('should copy an object to a new bucket and overwrite an already ' +
            'existing object in the destination bucket', done => {
            s3.send(new PutObjectCommand({ Bucket: destBucketName, Key: destObjName,
                Body: 'overwrite me', Metadata: originalMetadata })).then(() => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: newMetadata,
                })).then(res => {
                        assert.strictEqual(res.CopyObjectResult.ETag, etag);
                        s3.send(new GetObjectCommand({ Bucket: destBucketName,
                            Key: destObjName })).then(async res => {
                            assert.deepStrictEqual(res.Metadata,
                                newMetadata);
                            assert.strictEqual(res.ETag, etag);
                            const bodyString = await res.Body.transformToString();
                            assert.strictEqual(bodyString, content);
                            done();
                        }).catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                }
            );
        });

        // skipping test as object level encryption is not implemented yet
        it.skip('should copy an object and change the server side encryption' +
            'option if server side encryption header provided', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                ServerSideEncryption: 'AES256',
            })).then(() => {
                s3.send(new GetObjectCommand({ Bucket: destBucketName,
                    Key: destObjName })).then(res => {
                    assert.strictEqual(res.ServerSideEncryption,
                        'AES256');
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        it('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                SSECustomerAlgorithm: 'AES256' };
            s3.send(new CopyObjectCommand(params)).then(() => {
                throw Error('Expected NotImplemented error');
            }).catch(err => {
                assert.strictEqual(err.name, 'NotImplemented');
                done();
            });
        });

        it('should copy an object and set the acl on the new object', done => {
            s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                ACL: 'authenticated-read',
            })).then(() => {
                    s3.send(new GetObjectAclCommand({ Bucket: destBucketName,
                        Key: destObjName })).then(res => {
                        // With authenticated-read ACL, there are two
                        // grants:
                        // (1) FULL_CONTROL to the object owner
                        // (2) READ to the authenticated-read
                        assert.strictEqual(res.Grants.length, 2);
                        assert.strictEqual(res.Grants[0].Permission,
                            'FULL_CONTROL');
                        assert.strictEqual(res.Grants[1].Permission,
                            'READ');
                        assert.strictEqual(res.Grants[1].Grantee.URI,
                            'http://acs.amazonaws.com/groups/' +
                            'global/AuthenticatedUsers');
                        done();
                    }).catch(err => {
                        checkNoError(err);
                        done(err);
                    });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        it('should copy an object and default the acl on the new object ' +
            'to private even if the copied object had a ' +
            'different acl', done => {
            s3.send(new PutObjectAclCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                ACL: 'authenticated-read' })).then(() => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(() => {
                        s3.send(new GetObjectAclCommand({ Bucket: destBucketName,
                            Key: destObjName })).then(res => {
                            // With private ACL, there is only one grant
                            // of FULL_CONTROL to the object owner
                            assert.strictEqual(res.Grants.length, 1);
                            assert.strictEqual(res.Grants[0].Permission,
                                'FULL_CONTROL');
                            done();
                        }).catch(err => {
                            checkNoError(err);
                            done(err);
                        });
                }).catch(err => {
                    checkNoError(err);
                    done(err);
                });
            }).catch(err => {
                checkNoError(err);
                done(err);
            });
        });

        it('should return an error if attempt to copy with same source as' +
            'destination and do not change any metadata', done => {
            s3.send(new CopyObjectCommand({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
            })).then(() => {
                done();
            }).catch(err => {
                checkError(err, 'InvalidRequest', 400);
                done();
            });
        });

        it('should return an error if attempt to copy from nonexistent bucket',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `nobucket453234/${sourceObjName}`,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'NoSuchBucket', 404);
                    done();
                });
            });

        it('should return an error if use invalid redirect location',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    WebsiteRedirectLocation: 'google.com',
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'InvalidRedirectLocation', 400);
                    done();
                });
            });

        it('should return an error if copy request has object lock legal ' +
            'hold header but object lock is not enabled on destination bucket',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    ObjectLockLegalHoldStatus: 'ON',
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'InvalidRequest', 400);
                    done();
                });
            });

        it('should return an error if copy request has retention headers ' +
            'but object lock is not enabled on destination bucket',
            done => {
                const mockDate = new Date(2050, 10, 12);
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    ObjectLockMode: 'GOVERNANCE',
                    ObjectLockRetainUntilDate: mockDate,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'InvalidRequest', 400);
                    done();
                });
            });

        it('should return an error if attempt to copy to nonexistent bucket',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: 'nobucket453234', Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'NoSuchBucket', 404);
                    done();
                });
            });

        it('should return an error if attempt to copy nonexistent object',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey`,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'NoSuchKey', 404);
                    done();
                });
            });

        it('should return an error if attempt to copy nonexistent object',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/nokey`,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'NoSuchKey', 404);
                    done();
                });
            });

        it('should return an error if send invalid metadata directive header',
            done => {
                s3.send(new CopyObjectCommand({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'copyHalf',
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

        describe('copying by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            beforeEach(() => otherAccountBucketUtility
                .createOne(otherAccountBucket)
            );

            afterEach(() => otherAccountBucketUtility.empty(otherAccountBucket)
                .then(() => otherAccountBucketUtility
                .deleteOne(otherAccountBucket))
            );

            it('should not allow an account without read persmission on the ' +
                'source object to copy the object', done => {
                otherAccountS3.send(new CopyObjectCommand({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(() => {
                    done();
                }).catch(err => {
                    checkError(err, 'AccessDenied', 403);
                        done();
                    });
            });

            it('should not allow an account without write persmission on the ' +
                'destination bucket to copy the object', () => otherAccountS3.send(new PutObjectCommand(
                    { Bucket: otherAccountBucket,
                    Key: otherAccountKey, Body: '' })).then(() => otherAccountS3.send(new CopyObjectCommand(
                        { Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${otherAccountBucket}/${otherAccountKey}`,
                    })).catch(err => {
                        checkError(err, 'AccessDenied', 403);
                    })));


            it('should allow an account with read permission on the ' +
                'source object and write permission on the destination ' +
                'bucket to copy the object', () => s3.send(new PutObjectAclCommand({ Bucket: sourceBucketName,
                    Key: sourceObjName, ACL: 'public-read' })).then(() => otherAccountS3.send(new CopyObjectCommand(
                        { Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                    }))));
        });

        it('If-Match: returns no error when ETag match, with double quotes ' +
            'around ETag',
            done => {
                requestCopy({ CopySourceIfMatch: etag }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, with double ' +
            'quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch:
                    `non-matching,${etag}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match, without double ' +
            'quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch: etagTrim }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when one of ETags match, without ' +
            'double quotes around ETag',
            done => {
                requestCopy({ CopySourceIfMatch:
                    `non-matching,${etagTrim}` }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match: returns no error when ETag match with *', done => {
            requestCopy({ CopySourceIfMatch: '*' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match: returns PreconditionFailed when ETag does not match',
            done => {
                requestCopy({ CopySourceIfMatch: 'non-matching ETag' }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-None-Match: returns no error when ETag does not match', done => {
            requestCopy({ CopySourceIfNoneMatch: 'non-matching' }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match: returns no error when all ETags do not match',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: 'non-matching,non-matching-either',
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-None-Match: returns PreconditionFailed when ETag match, with' +
            'double quotes around ETag',
            done => {
                requestCopy({ CopySourceIfNoneMatch: etag }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-None-Match: returns PreconditionFailed when one of ETags ' +
            'match, with double quotes around ETag',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: `non-matching,${etag}`,
                }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-None-Match: returns PreconditionFailed when ETag match, ' +
            'without double quotes around ETag',
            done => {
                requestCopy({ CopySourceIfNoneMatch: etagTrim }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-None-Match: returns PreconditionFailed when one of ETags ' +
            'match, without double quotes around ETag',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: `non-matching,${etagTrim}`,
                }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-Modified-Since: returns no error if Last modified date is ' +
            'greater',
            done => {
                requestCopy({ CopySourceIfModifiedSince: dateFromNow(-1) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Modified-Since: returns PreconditionFailed if Last ' +
            'modified date is lesser',
            done => {
                requestCopy({ CopySourceIfModifiedSince: dateFromNow(1) },
                    err => {
                        checkError(err, 'PreconditionFailed', 412);
                        done();
                    });
            });

        it('If-Modified-Since: returns PreconditionFailed if Last modified ' +
            'date is equal',
            done => {
                requestCopy({ CopySourceIfModifiedSince:
                    dateConvert(lastModified) },
                    err => {
                        checkError(err, 'PreconditionFailed', 412);
                        done();
                    });
            });

        it('If-Unmodified-Since: returns no error when lastModified date is ' +
            'greater',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(1) },
                err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Unmodified-Since: returns no error when lastModified ' +
            'date is equal',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince:
                    dateConvert(lastModified) },
                    err => {
                        checkNoError(err);
                        done();
                    });
            });

        it('If-Unmodified-Since: returns PreconditionFailed when ' +
            'lastModified date is lesser',
            done => {
                requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(-1) },
                err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-Match & If-Unmodified-Since: returns no error when match Etag ' +
            'and lastModified is greater',
            done => {
                requestCopy({
                    CopySourceIfMatch: etagTrim,
                    CopySourceIfUnmodifiedSince: dateFromNow(-1),
                }, err => {
                    checkNoError(err);
                    done();
                });
            });

        it('If-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-Match not match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-Match not match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-None-Match & If-Modified-Since: returns PreconditionFailed ' +
            'when Etag does not match and lastModified is greater',
            done => {
                requestCopy({
                    CopySourceIfNoneMatch: etagTrim,
                    CopySourceIfModifiedSince: dateFromNow(-1),
                }, err => {
                    checkError(err, 'PreconditionFailed', 412);
                    done();
                });
            });

        it('If-None-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-None-Match match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        it.skip('If-None-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        it('If-None-Match match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('If-None-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed', 412);
                done();
            });
        });

        it('should return InvalidStorageClass error when x-amz-storage-class header is provided ' +
        'and not equal to STANDARD', done => {
            s3.send(new CopyObjectCommand({
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                StorageClass: 'COLD',
            })).then(() => {
                throw new Error('Expected InvalidStorageClass error');
            }).catch(err => {
                assert.strictEqual(err.name, 'InvalidStorageClass');
                assert.strictEqual(err.$metadata.httpStatusCode, 400);
                done();
            });
        });

        it('should not copy a cold object', done => {
            const archive = {
                archiveInfo: {
                    archiveId: '97a71dfe-49c1-4cca-840a-69199e0b0322',
                    archiveVersion: 5577006791947779
                },
            };
            fakeMetadataArchive(sourceBucketName, sourceObjName, undefined, archive, err => {
                assert.ifError(err);
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(() => {
                throw new Error('Expected InvalidObjectState error');
            }).catch(err => {
                assert.strictEqual(err.name, 'InvalidObjectState');
                assert.strictEqual(err.$metadata.httpStatusCode, 403);
                done();
                });
            });
        });

        it('should copy an object when it\'s transitioning to cold', done => {
            fakeMetadataTransition(sourceBucketName, sourceObjName, undefined, err => {
                assert.ifError(err);
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(async res => {
                    await successCopyCheck(null, res.CopyObjectResult, originalMetadata,
                        destBucketName, destObjName);
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done();
                });
            });
        });

        it('should copy restored object and reset storage class', done => {
            const archiveCompleted = {
                archiveInfo: {},
                restoreRequestedAt: new Date(0),
                restoreRequestedDays: 5,
                restoreCompletedAt: new Date(10),
                restoreWillExpireAt: new Date(10 + (5 * 24 * 60 * 60 * 1000)),
            };
            fakeMetadataArchive(sourceBucketName, sourceObjName, undefined, archiveCompleted, err => {
                assert.ifError(err);
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                })).then(async res => {
                    await successCopyCheck(null, res.CopyObjectResult, originalMetadata,
                        destBucketName, destObjName);
                    done();
                }).catch(err => {
                    checkNoError(err);
                    done();
                });
            });
        });
    });
});


describe('Object Copy with object lock enabled on both destination ' +
    'bucket and source bucket', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let versionId;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return bucketUtil.empty(sourceBucketName, true)
                .then(() => bucketUtil.empty(destBucketName))
                .then(() =>
                    bucketUtil.deleteMany([sourceBucketName, destBucketName]))
                .catch(err => {
                    if (err.name !== 'NoSuchBucket') {
                        process.stdout.write(`${err}\n`);
                        throw err;
                    }
                })
                .then(() => bucketUtil.createOneWithLock(sourceBucketName))
                .then(() => bucketUtil.createOneWithLock(destBucketName))
                .catch(err => {
                    throw err;
                });
        });

        beforeEach(() => s3.send(new PutObjectCommand({
            Bucket: sourceBucketName,
            Key: sourceObjName,
            Body: content,
            Metadata: originalMetadata,
            ObjectLockMode: 'GOVERNANCE',
            ObjectLockRetainUntilDate: new Date(2050, 1, 1),
        })).then(res => {
            versionId = res.VersionId;
            s3.send(new HeadObjectCommand({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            }));
        }));

        afterEach(async () => {
            await bucketUtil.empty(sourceBucketName);
            await bucketUtil.empty(destBucketName);
        });

        after(async () => await bucketUtil.deleteMany([sourceBucketName, destBucketName]));

        it('should not copy default retention info of the destination ' +
            'bucket if legal hold header is passed with copy object request',
            done => {
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    ObjectLockLegalHoldStatus: 'ON',
                })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }))
                        .then(res => {
                            assert.strictEqual(res.ObjectLockMode, undefined);
                            assert.strictEqual(res.ObjectLockRetainUntilDate,
                                undefined);
                            assert.strictEqual(res.ObjectLockLegalHoldStatus,
                                'ON');
                            const removeLockObjs = [
                                {
                                    bucket: sourceBucketName,
                                    key: sourceObjName,
                                    versionId,
                                }, {
                                    bucket: destBucketName,
                                    key: destObjName,
                                    versionId: res.VersionId,
                                },
                            ];
                            new Promise((resolve, reject) => {
                                changeObjectLock(removeLockObjs, '', err => {
                                    if (err) {
                                        reject(err);
                                    } else {
                                        resolve();
                                    }
                                });
                            }).then(done).catch(err => {
                                assert.ifError(err);
                                done(err);
                            });
                        }).catch(err => {
                    assert.ifError(err);
                    done(err);
                });
            }).catch(err => {
                assert.ifError(err);
                done(err);
            });
        });

        it('should not copy default retention info of the destination ' +
            'bucket if legal hold header is passed with copy object request',
            done => {
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    ObjectLockLegalHoldStatus: 'on',
                })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }))
                        .then(res => {
                            assert.strictEqual(res.ObjectLockMode, undefined);
                            assert.strictEqual(res.ObjectLockMode, undefined);
                            assert.strictEqual(res.ObjectLockRetainUntilDate,
                                undefined);
                            assert.strictEqual(res.ObjectLockLegalHoldStatus,
                                'OFF');
                            const removeLockObjs = [
                                {
                                    bucket: sourceBucketName,
                                    key: sourceObjName,
                                    versionId,
                                },
                            ];
                            changeObjectLock(removeLockObjs, '', done);
                        }).catch(err => {
                            assert.ifError(err);
                            done(err);
                        });
                }).catch(err => {
                    assert.ifError(err);
                    done(err);
                });
            });

        it('should overwrite default retention info of the destination ' +
            'bucket if retention headers passed with copy object request',
            done => {
                s3.send(new CopyObjectCommand({
                    Bucket: destBucketName,
                    Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    ObjectLockMode: 'COMPLIANCE',
                    ObjectLockRetainUntilDate: new Date(2055, 2, 3),
                })).then(() => {
                    s3.send(new GetObjectCommand({ Bucket: destBucketName, Key: destObjName }))
                        .then(res => {
                            assert.strictEqual(res.ObjectLockMode, 'COMPLIANCE');
                            assert.strictEqual(res.ObjectLockRetainUntilDate.toGMTString(),
                                new Date(2055, 2, 3).toGMTString());
                            const removeLockObjs = [
                                {
                                    bucket: sourceBucketName,
                                    key: sourceObjName,
                                    versionId,
                                }, {
                                    bucket: destBucketName,
                                    key: destObjName,
                                    versionId: res.VersionId,
                                },
                            ];
                            changeObjectLock(removeLockObjs, '', done);
                        }).catch(err => {
                            assert.ifError(err);
                            done(err);
                        });
                }).catch(err => {
                    assert.ifError(err);
                    done(err);
                });
            });
        });

});
