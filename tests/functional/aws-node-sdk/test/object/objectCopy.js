const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const { taggingTests } = require('../../lib/utility/tagging');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');

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

function checkNoError(err) {
    expect(err).toEqual(null);
}

function checkError(err, code) {
    expect(err).not.toEqual(null);
    expect(err.code).toBe(code);
}

function dateFromNow(diff) {
    const d = new Date();
    d.setHours(d.getHours() + diff);
    return d.toISOString();
}

function dateConvert(d) {
    return (new Date(d)).toISOString();
}


describe('Object Copy', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let etag;
        let etagTrim;
        let lastModified;

        beforeAll(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return bucketUtil.empty(sourceBucketName)
            .then(() =>
                bucketUtil.empty(destBucketName)
            )
            .then(() =>
                bucketUtil.deleteMany([sourceBucketName, destBucketName])
            )
            .catch(err => {
                if (err.code !== 'NoSuchBucket') {
                    process.stdout.write(`${err}\n`);
                    throw err;
                }
            })
            .then(() => bucketUtil.createOne(sourceBucketName)
            )
            .then(() => bucketUtil.createOne(destBucketName)
            )
            .catch(err => {
                throw err;
            });
        });

        beforeEach(() => s3.putObjectAsync({
            Bucket: sourceBucketName,
            Key: sourceObjName,
            Body: content,
            Metadata: originalMetadata,
            CacheControl: originalCacheControl,
            ContentDisposition: originalContentDisposition,
            ContentEncoding: originalContentEncoding,
            Expires: originalExpires,
            Tagging: originalTagging,
        }).then(res => {
            etag = res.ETag;
            etagTrim = etag.substring(1, etag.length - 1);
            return s3.headObjectAsync({
                Bucket: sourceBucketName,
                Key: sourceObjName,
            });
        }).then(res => {
            lastModified = res.LastModified;
        }));

        afterEach(() => bucketUtil.empty(sourceBucketName)
            .then(() => bucketUtil.empty(destBucketName)));

        afterAll(() => bucketUtil.deleteMany([sourceBucketName, destBucketName]));

        function requestCopy(fields, cb) {
            s3.copyObject(Object.assign({
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
            }, fields), cb);
        }

        function successCopyCheck(error, response, copyVersionMetadata,
            destBucketName, destObjName, done) {
            checkNoError(error);
            expect(response.ETag).toBe(etag);
            const copyLastModified = new Date(response.LastModified)
                .toUTCString();
            s3.getObject({ Bucket: destBucketName,
                Key: destObjName }, (err, res) => {
                checkNoError(err);
                expect(res.Body.toString()).toBe(content);
                assert.deepStrictEqual(res.Metadata,
                    copyVersionMetadata);
                expect(res.LastModified).toBe(copyLastModified);
                done();
            });
        }

        function checkSuccessTagging(key, value, cb) {
            s3.getObjectTagging({ Bucket: destBucketName, Key: destObjName },
            (err, data) => {
                checkNoError(err);
                expect(data.TagSet[0].Key).toBe(key);
                expect(data.TagSet[0].Value).toBe(value);
                cb();
            });
        }

        function checkNoTagging(cb) {
            s3.getObjectTagging({ Bucket: destBucketName, Key: destObjName },
            (err, data) => {
                checkNoError(err);
                expect(data.TagSet.length).toBe(0);
                cb();
            });
        }

        test('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the metadata if no metadata directve' +
            'header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` },
                (err, res) =>
                    successCopyCheck(err, res, originalMetadata,
                        destBucketName, destObjName, done)
                );
        });

        test('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if no tagging directive' +
            'header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                });
        });

        test('should return 400 InvalidArgument if invalid tagging ' +
        'directive', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COCO' },
                err => {
                    checkError(err, 'InvalidArgument');
                    done();
                });
        });

        test('should copy an object from a source bucket to a different ' +
            'destination bucket and copy the tag set if COPY tagging ' +
            'directive header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COPY' },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(originalTagKey, originalTagValue, done);
                });
        });

        test('should copy an object and tag set if COPY ' +
            'included as tag directive header (and ignore any new ' +
            'tag set sent with copy request)', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'COPY',
                Tagging: newTagging,
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        assert.deepStrictEqual(res.Metadata, originalMetadata);
                        done();
                    });
                });
        });

        test('should copy an object from a source to the same destination ' +
        'updating tag if REPLACE tagging directive header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE', Tagging: newTagging },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(newTagKey, newTagValue, done);
                });
        });

        test('should copy an object from a source to the same destination ' +
        'return no tag if REPLACE tagging directive header provided but ' +
        '"x-amz-tagging" header is not specified', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE' },
                err => {
                    checkNoError(err);
                    checkNoTagging(done);
                });
        });

        test('should copy an object from a source to the same destination ' +
        'return no tag if COPY tagging directive header but provided from ' +
        'an empty object', done => {
            s3.putObject({ Bucket: sourceBucketName, Key: 'emptyobject' },
            err => {
                checkNoError(err);
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/emptyobject`,
                    TaggingDirective: 'COPY' },
                    err => {
                        checkNoError(err);
                        checkNoTagging(done);
                    });
            });
        });

        test('should copy an object from a source to the same destination ' +
        'updating tag if REPLACE tagging directive header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                TaggingDirective: 'REPLACE', Tagging: newTagging },
                err => {
                    checkNoError(err);
                    checkSuccessTagging(newTagKey, newTagValue, done);
                });
        });

        describe('Copy object updating tag set', () => {
            taggingTests.forEach(taggingTest => {
                test(taggingTest.it, done => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: destBucketName, Key: destObjName,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                        TaggingDirective: 'REPLACE', Tagging: tagging };
                    s3.copyObject(params, err => {
                        if (taggingTest.error) {
                            checkError(err, taggingTest.error);
                            return done();
                        }
                        expect(err).toEqual(null);
                        return checkSuccessTagging(taggingTest.tag.key,
                          taggingTest.tag.value, done);
                    });
                });
            });
        });

        test('should also copy additional headers (CacheControl, ' +
        'ContentDisposition, ContentEncoding, Expires) when copying an ' +
        'object from a source bucket to a different destination bucket', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName, Key: destObjName },
                      (err, res) => {
                          if (err) {
                              done(err);
                          }
                          expect(res.CacheControl).toBe(originalCacheControl);
                          expect(res.ContentDisposition).toBe(originalContentDisposition);
                          // Should remove V4 streaming value 'aws-chunked'
                          // to be compatible with AWS behavior
                          expect(res.ContentEncoding).toBe('base64,');
                          expect(res.Expires).toBe(originalExpires.toGMTString());
                          done();
                      });
                });
        });

        test('should copy an object from a source bucket to a different ' +
            'key in the same bucket', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}` },
                (err, res) =>
                    successCopyCheck(err, res, originalMetadata,
                        sourceBucketName, destObjName, done)
                );
        });

        test('should not return error if copying object w/ > ' +
        '2KB user-defined md and COPY directive', done => {
            const metadata = genMaxSizeMetaHeaders();
            const params = {
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'COPY',
                Metadata: metadata,
            };
            s3.copyObject(params, err => {
                expect(err).toBe(null);
                // add one more byte to be over the limit
                metadata.header0 = `${metadata.header0}${'0'}`;
                s3.copyObject(params, err => {
                    expect(err).toBe(null);
                    done();
                });
            });
        });

        test('should return error if copying object w/ > 2KB ' +
        'user-defined md and REPLACE directive', done => {
            const metadata = genMaxSizeMetaHeaders();
            const params = {
                Bucket: destBucketName,
                Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                Metadata: metadata,
            };
            s3.copyObject(params, err => {
                expect(err).toBe(null);
                // add one more byte to be over the limit
                metadata.header0 = `${metadata.header0}${'0'}`;
                s3.copyObject(params, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('MetadataTooLarge');
                    expect(err.statusCode).toBe(400);
                    done();
                });
            });
        });

        test('should copy an object from a source to the same destination ' +
            '(update metadata)', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata },
                (err, res) =>
                    successCopyCheck(err, res, newMetadata,
                        sourceBucketName, sourceObjName, done)
                );
        });

        test('should copy an object and replace the metadata if replace ' +
            'included as metadata directive header', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                Metadata: newMetadata,
            },
                (err, res) =>
                    successCopyCheck(err, res, newMetadata,
                        destBucketName, destObjName, done)
                );
        });

        test('should copy an object and replace ContentType if replace ' +
            'included as a metadata directive header, and new ContentType is ' +
            'provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                ContentType: 'image',
            }, () => {
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.ContentType).toBe('image');
                    return done();
                });
            });
        });

        test('should copy an object and keep ContentType if replace ' +
            'included as a metadata directive header, but no new ContentType ' +
            'is provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
            }, () => {
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res.ContentType).toBe('application/octet-stream');
                    return done();
                });
            });
        });

        test('should also replace additional headers if replace ' +
            'included as metadata directive header and new headers are ' +
            'specified', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'REPLACE',
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            }, err => {
                checkNoError(err);
                s3.getObject({ Bucket: destBucketName,
                    Key: destObjName }, (err, res) => {
                    if (err) {
                        done(err);
                    }
                    expect(res.CacheControl).toBe(newCacheControl);
                    expect(res.ContentDisposition).toBe(newContentDisposition);
                    // Should remove V4 streaming value 'aws-chunked'
                    // to be compatible with AWS behavior
                    expect(res.ContentEncoding).toBe('gzip,');
                    expect(res.Expires).toBe(newExpires.toGMTString());
                    done();
                });
            });
        });

        test('should copy an object and the metadata if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'metadata sent with copy request)', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        assert.deepStrictEqual(res.Metadata, originalMetadata);
                        done();
                    });
                });
        });

        test('should copy an object and its additional headers if copy ' +
            'included as metadata directive header (and ignore any new ' +
            'headers sent with copy request)', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                MetadataDirective: 'COPY',
                Metadata: newMetadata,
                CacheControl: newCacheControl,
                ContentDisposition: newContentDisposition,
                ContentEncoding: newContentEncoding,
                Expires: newExpires,
            }, err => {
                checkNoError(err);
                s3.getObject({ Bucket: destBucketName, Key: destObjName },
                  (err, res) => {
                      if (err) {
                          done(err);
                      }
                      expect(res.CacheControl).toBe(originalCacheControl);
                      expect(res.ContentDisposition).toBe(originalContentDisposition);
                      expect(res.ContentEncoding).toBe('base64,');
                      expect(res.Expires).toBe(originalExpires.toGMTString());
                      done();
                  });
            });
        });

        test('should copy a 0 byte object to different destination', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.putObject({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '', Metadata: originalMetadata }, () => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                    (err, res) => {
                        checkNoError(err);
                        expect(res.ETag).toBe(emptyFileETag);
                        s3.getObject({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            checkNoError(err);
                            assert.deepStrictEqual(res.Metadata,
                                originalMetadata);
                            expect(res.ETag).toBe(emptyFileETag);
                            done();
                        });
                    });
            });
        });

        test('should copy a 0 byte object to same destination', done => {
            const emptyFileETag = '"d41d8cd98f00b204e9800998ecf8427e"';
            s3.putObject({ Bucket: sourceBucketName, Key: sourceObjName,
                Body: '' }, () => {
                s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    StorageClass: 'REDUCED_REDUNDANCY',
                },
                    (err, res) => {
                        checkNoError(err);
                        expect(res.ETag).toBe(emptyFileETag);
                        s3.getObject({ Bucket: sourceBucketName,
                            Key: sourceObjName }, (err, res) => {
                            assert.deepStrictEqual(res.Metadata,
                                {});
                            assert.deepStrictEqual(res.StorageClass,
                                'REDUCED_REDUNDANCY');
                            expect(res.ETag).toBe(emptyFileETag);
                            done();
                        });
                    });
            });
        });

        test('should copy an object to a different destination and change ' +
            'the storage class if storage class header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                StorageClass: 'REDUCED_REDUNDANCY',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        expect(res.StorageClass).toBe('REDUCED_REDUNDANCY');
                        done();
                    });
                });
        });

        test('should copy an object to the same destination and change the ' +
            'storage class if the storage class header provided', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                StorageClass: 'REDUCED_REDUNDANCY',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: sourceBucketName,
                        Key: sourceObjName }, (err, res) => {
                        checkNoError(err);
                        expect(res.StorageClass).toBe('REDUCED_REDUNDANCY');
                        done();
                    });
                });
        });

        test('should copy an object to a new bucket and overwrite an already ' +
            'existing object in the destination bucket', done => {
            s3.putObject({ Bucket: destBucketName, Key: destObjName,
                Body: 'overwrite me', Metadata: originalMetadata }, () => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: newMetadata,
                },
                    (err, res) => {
                        checkNoError(err);
                        expect(res.ETag).toBe(etag);
                        s3.getObject({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            assert.deepStrictEqual(res.Metadata,
                                newMetadata);
                            expect(res.ETag).toBe(etag);
                            expect(res.Body.toString()).toBe(content);
                            done();
                        });
                    });
            });
        });

        // skipping test as object level encryption is not implemented yet
        test.skip('should copy an object and change the server side encryption' +
            'option if server side encryption header provided', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                ServerSideEncryption: 'AES256',
            },
                err => {
                    checkNoError(err);
                    s3.getObject({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        expect(res.ServerSideEncryption).toBe('AES256');
                        done();
                    });
                });
        });

        test('should return Not Implemented error for obj. encryption using ' +
            'AWS-managed encryption keys', done => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                ServerSideEncryption: 'AES256' };
            s3.copyObject(params, err => {
                expect(err.code).toBe('NotImplemented');
                done();
            });
        });

        test('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: destBucketName, Key: 'key',
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                SSECustomerAlgorithm: 'AES256' };
            s3.copyObject(params, err => {
                expect(err.code).toBe('NotImplemented');
                done();
            });
        });

        test('should copy an object and set the acl on the new object', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                ACL: 'authenticated-read',
            },
                err => {
                    checkNoError(err);
                    s3.getObjectAcl({ Bucket: destBucketName,
                        Key: destObjName }, (err, res) => {
                        // With authenticated-read ACL, there are two
                        // grants:
                        // (1) FULL_CONTROL to the object owner
                        // (2) READ to the authenticated-read
                        expect(res.Grants.length).toBe(2);
                        expect(res.Grants[0].Permission).toBe('FULL_CONTROL');
                        expect(res.Grants[1].Permission).toBe('READ');
                        expect(res.Grants[1].Grantee.URI).toBe('http://acs.amazonaws.com/groups/' +
                        'global/AuthenticatedUsers');
                        done();
                    });
                });
        });

        test('should copy an object and default the acl on the new object ' +
            'to private even if the copied object had a ' +
            'different acl', done => {
            s3.putObjectAcl({ Bucket: sourceBucketName, Key: sourceObjName,
                ACL: 'authenticated-read' }, () => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                    () => {
                        s3.getObjectAcl({ Bucket: destBucketName,
                            Key: destObjName }, (err, res) => {
                            // With private ACL, there is only one grant
                            // of FULL_CONTROL to the object owner
                            expect(res.Grants.length).toBe(1);
                            expect(res.Grants[0].Permission).toBe('FULL_CONTROL');
                            done();
                        });
                    });
            });
        });

        test('should return an error if attempt to copy with same source as' +
            'destination and do not change any metadata', done => {
            s3.copyObject({ Bucket: sourceBucketName, Key: sourceObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
            },
                err => {
                    checkError(err, 'InvalidRequest');
                    done();
                });
        });

        test(
            'should return an error if attempt to copy from nonexistent bucket',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `nobucket453234/${sourceObjName}`,
                },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            }
        );

        test('should return an error if use invalid redirect location', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/${sourceObjName}`,
                WebsiteRedirectLocation: 'google.com',
            },
            err => {
                checkError(err, 'InvalidRedirectLocation');
                done();
            });
        });


        test(
            'should return an error if attempt to copy to nonexistent bucket',
            done => {
                s3.copyObject({ Bucket: 'nobucket453234', Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                err => {
                    checkError(err, 'NoSuchBucket');
                    done();
                });
            }
        );

        test('should return an error if attempt to copy nonexistent object', done => {
            s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                CopySource: `${sourceBucketName}/nokey`,
            },
            err => {
                checkError(err, 'NoSuchKey');
                done();
            });
        });

        test(
            'should return an error if send invalid metadata directive header',
            done => {
                s3.copyObject({ Bucket: destBucketName, Key: destObjName,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                    MetadataDirective: 'copyHalf',
                },
                err => {
                    checkError(err, 'InvalidArgument');
                    done();
                });
            }
        );

        describe('copying by another account', () => {
            const otherAccountBucket = 'otheraccountbucket42342342342';
            const otherAccountKey = 'key';
            beforeEach(() => otherAccountBucketUtility
                .createOne(otherAccountBucket));

            afterEach(() => otherAccountBucketUtility.empty(otherAccountBucket)
                .then(() => otherAccountBucketUtility
                .deleteOne(otherAccountBucket)));

            test('should not allow an account without read persmission on the ' +
                'source object to copy the object', done => {
                otherAccountS3.copyObject({ Bucket: otherAccountBucket,
                    Key: otherAccountKey,
                    CopySource: `${sourceBucketName}/${sourceObjName}`,
                },
                    err => {
                        checkError(err, 'AccessDenied');
                        done();
                    });
            });

            test('should not allow an account without write persmission on the ' +
                'destination bucket to copy the object', done => {
                otherAccountS3.putObject({ Bucket: otherAccountBucket,
                    Key: otherAccountKey, Body: '' }, () => {
                    otherAccountS3.copyObject({ Bucket: destBucketName,
                        Key: destObjName,
                        CopySource: `${otherAccountBucket}/${otherAccountKey}`,
                    },
                        err => {
                            checkError(err, 'AccessDenied');
                            done();
                        });
                });
            });

            test('should allow an account with read permission on the ' +
                'source object and write permission on the destination ' +
                'bucket to copy the object', done => {
                s3.putObjectAcl({ Bucket: sourceBucketName,
                    Key: sourceObjName, ACL: 'public-read' }, () => {
                    otherAccountS3.copyObject({ Bucket: otherAccountBucket,
                        Key: otherAccountKey,
                        CopySource: `${sourceBucketName}/${sourceObjName}`,
                    },
                        err => {
                            checkNoError(err);
                            done();
                        });
                });
            });
        });

        test('If-Match: returns no error when ETag match, with double quotes ' +
            'around ETag', done => {
            requestCopy({ CopySourceIfMatch: etag }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match: returns no error when one of ETags match, with double ' +
            'quotes around ETag', done => {
            requestCopy({ CopySourceIfMatch:
                `non-matching,${etag}` }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match: returns no error when ETag match, without double ' +
            'quotes around ETag', done => {
            requestCopy({ CopySourceIfMatch: etagTrim }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match: returns no error when one of ETags match, without ' +
            'double quotes around ETag', done => {
            requestCopy({ CopySourceIfMatch:
                `non-matching,${etagTrim}` }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match: returns no error when ETag match with *', done => {
            requestCopy({ CopySourceIfMatch: '*' }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match: returns PreconditionFailed when ETag does not match', done => {
            requestCopy({ CopySourceIfMatch: 'non-matching ETag' }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match: returns no error when ETag does not match', done => {
            requestCopy({ CopySourceIfNoneMatch: 'non-matching' }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-None-Match: returns no error when all ETags do not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching,non-matching-either',
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-None-Match: returns PreconditionFailed when ETag match, with' +
            'double quotes around ETag', done => {
            requestCopy({ CopySourceIfNoneMatch: etag }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match: returns PreconditionFailed when one of ETags ' +
            'match, with double quotes around ETag', done => {
            requestCopy({
                CopySourceIfNoneMatch: `non-matching,${etag}`,
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match: returns PreconditionFailed when ETag match, ' +
            'without double quotes around ETag', done => {
            requestCopy({ CopySourceIfNoneMatch: etagTrim }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match: returns PreconditionFailed when one of ETags ' +
            'match, without double quotes around ETag', done => {
            requestCopy({
                CopySourceIfNoneMatch: `non-matching,${etagTrim}`,
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-Modified-Since: returns no error if Last modified date is ' +
            'greater', done => {
            requestCopy({ CopySourceIfModifiedSince: dateFromNow(-1) },
                err => {
                    checkNoError(err);
                    done();
                });
        });

        // Skipping this test, because real AWS does not provide error as
        // expected
        test.skip('If-Modified-Since: returns PreconditionFailed if Last ' +
            'modified date is lesser', done => {
            requestCopy({ CopySourceIfModifiedSince: dateFromNow(1) },
                err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
        });

        test('If-Modified-Since: returns PreconditionFailed if Last modified ' +
            'date is equal', done => {
            requestCopy({ CopySourceIfModifiedSince:
                dateConvert(lastModified) },
                err => {
                    checkError(err, 'PreconditionFailed');
                    done();
                });
        });

        test('If-Unmodified-Since: returns no error when lastModified date is ' +
            'greater', done => {
            requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(1) },
            err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Unmodified-Since: returns no error when lastModified ' +
            'date is equal', done => {
            requestCopy({ CopySourceIfUnmodifiedSince:
                dateConvert(lastModified) },
                err => {
                    checkNoError(err);
                    done();
                });
        });

        test('If-Unmodified-Since: returns PreconditionFailed when ' +
            'lastModified date is lesser', done => {
            requestCopy({ CopySourceIfUnmodifiedSince: dateFromNow(-1) },
            err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-Match & If-Unmodified-Since: returns no error when match Etag ' +
            'and lastModified is greater', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-Match not match & If-Unmodified-Since match', done => {
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
        test.skip('If-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-Match not match & If-Modified-Since match', done => {
            requestCopy({
                CopySourceIfMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match & If-Modified-Since: returns PreconditionFailed ' +
            'when Etag does not match and lastModified is greater', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match not match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match match & If-Modified-Since match', done => {
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
        test.skip('If-None-Match match & If-Modified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfModifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkNoError(err);
                done();
            });
        });

        test('If-None-Match match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: 'non-matching',
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match not match & If-Unmodified-Since match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });

        test('If-None-Match not match & If-Unmodified-Since not match', done => {
            requestCopy({
                CopySourceIfNoneMatch: etagTrim,
                CopySourceIfUnmodifiedSince: dateFromNow(-1),
            }, err => {
                checkError(err, 'PreconditionFailed');
                done();
            });
        });
    });
});
