const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    createDualNullVersion,
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
    checkOneVersion,
} = require('../../lib/utility/versioning-util');

const customS3Request = require('../../lib/utility/customS3Request');

const data = ['foo1', 'foo2'];
const counter = 100;
const key = 'objectKey';

function _assertNoError(err, desc) {
    expect(err).toBe(null);
}


describe('put and get object with versioning', () => {
    this.timeout(600000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let bucket;

        beforeEach(done => {
            bucket = `versioning-bucket-${Date.now()}`;
            s3.createBucket({ Bucket: bucket }, done);
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, done);
            });
        });

        test(
            'should return InvalidArgument for a request with versionId query',
            done => {
                const params = { Bucket: bucket, Key: key };
                const query = { versionId: 'testVersionId' };
                customS3Request(s3.putObject, params, { query }, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('InvalidArgument');
                    expect(err.statusCode).toBe(400);
                    done();
                });
            }
        );

        test('should return InvalidArgument for a request with empty string ' +
        'versionId query', done => {
            const params = { Bucket: bucket, Key: key };
            const query = { versionId: '' };
            customS3Request(s3.putObject, params, { query }, err => {
                expect(err).toBeTruthy();
                expect(err.code).toBe('InvalidArgument');
                expect(err.statusCode).toBe(400);
                done();
            });
        });

        test('should put and get a non-versioned object without including ' +
        'version ids in response headers', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                expect(data.VersionId).toBe(undefined);
                s3.getObject(params, (err, data) => {
                    _assertNoError(err, 'getting object');
                    expect(data.VersionId).toBe(undefined);
                    done();
                });
            });
        });

        test('version-specific get should still not return version id in ' +
        'response header', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                expect(data.VersionId).toBe(undefined);
                params.VersionId = 'null';
                s3.getObject(params, (err, data) => {
                    _assertNoError(err, 'getting specific version "null"');
                    expect(data.VersionId).toBe(undefined);
                    done();
                });
            });
        });

        describe('on a version-enabled bucket', () => {
            beforeEach(done => {
                s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }, done);
            });

            test('should create a new version for an object', done => {
                const params = { Bucket: bucket, Key: key };
                s3.putObject(params, (err, data) => {
                    _assertNoError(err, 'putting object');
                    params.VersionId = data.VersionId;
                    s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting object');
                        expect(params.VersionId).toBe(data.VersionId);
                        done();
                    });
                });
            });

            test('should create a new version with tag set for an object', done => {
                const tagKey = 'key1';
                const tagValue = 'value1';
                const putParams = { Bucket: bucket, Key: key,
                    Tagging: `${tagKey}=${tagValue}` };
                s3.putObject(putParams, (err, data) => {
                    _assertNoError(err, 'putting object');
                    const getTagParams = { Bucket: bucket, Key:
                      key, VersionId: data.VersionId };
                    s3.getObjectTagging(getTagParams, (err, data) => {
                        _assertNoError(err, 'getting object tagging');
                        expect(getTagParams.VersionId).toBe(data.VersionId);
                        expect(data.TagSet[0].Key).toBe(tagKey);
                        expect(data.TagSet[0].Value).toBe(tagValue);
                        done();
                    });
                });
            });
        });

        describe('on a version-enabled bucket with non-versioned object',
        () => {
            const eTags = [];

            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: key, Body: data[0] },
                    (err, data) => {
                        if (err) {
                            done(err);
                        }
                        eTags.push(data.ETag);
                        s3.putBucketVersioning({
                            Bucket: bucket,
                            VersioningConfiguration: versioningEnabled,
                        }, done);
                    });
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            test('should get null (latest) version in versioning enabled ' +
            'bucket when version id is not specified', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                };
                s3.getObject(paramsNull, (err, data) => {
                    _assertNoError(err, 'getting null version');
                    expect(data.VersionId).toBe('null');
                    done();
                });
            });

            test('should get null version in versioning enabled bucket ' +
            'when version id is specified', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.getObject(paramsNull, (err, data) => {
                    _assertNoError(err, 'getting null version');
                    expect(data.VersionId).toBe('null');
                    done();
                });
            });

            test('should keep null version and create a new version', done => {
                const params = { Bucket: bucket, Key: key, Body: data[1] };
                s3.putObject(params, (err, data) => {
                    const newVersion = data.VersionId;
                    eTags.push(data.ETag);
                    s3.getObject({ Bucket: bucket, Key: key,
                        VersionId: newVersion }, (err, data) => {
                        expect(err).toBe(null);
                        expect(data.VersionId).toBe(newVersion);
                        expect(data.ETag).toBe(eTags[1]);
                        s3.getObject({ Bucket: bucket, Key: key,
                            VersionId: 'null' }, (err, data) => {
                            _assertNoError(err, 'getting null version');
                            expect(data.VersionId).toBe('null');
                            expect(data.ETag).toBe(eTags[0]);
                            done();
                        });
                    });
                });
            });

            test('should create new versions but still keep nullVersionId', done => {
                const versionIds = [];
                const params = { Bucket: bucket, Key: key };
                const paramsNull = {
                    Bucket: bucket, Key:
                    key,
                    VersionId: 'null',
                };
                // create new versions
                async.timesSeries(counter, (i, next) => s3.putObject(params,
                    (err, data) => {
                        versionIds.push(data.VersionId);
                        // get the 'null' version
                        s3.getObject(paramsNull, (err, nullVerData) => {
                            expect(err).toBe(null);
                            expect(nullVerData.ETag).toBe(eTags[0]);
                            expect(nullVerData.VersionId).toBe('null');
                            next(err);
                        });
                    }), done);
            });
        });

        describe('on version-suspended bucket', () => {
            beforeEach(done => {
                s3.putBucketVersioning({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }, done);
            });

            test('should not return version id for new object', done => {
                const params = { Bucket: bucket, Key: key, Body: 'foo' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.putObject(params, (err, data) => {
                    const eTag = data.ETag;
                    _assertNoError(err, 'putting object');
                    expect(data.VersionId).toBe(undefined);
                    // getting null version should return object we just put
                    s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        expect(nullVerData.ETag).toBe(eTag);
                        expect(nullVerData.VersionId).toBe('null');
                        done();
                    });
                });
            });

            test('should update null version if put object twice', done => {
                const params = { Bucket: bucket, Key: key };
                const params1 = { Bucket: bucket, Key: key, Body: data[0] };
                const params2 = { Bucket: bucket, Key: key, Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                const eTags = [];
                async.waterfall([
                    callback => s3.putObject(params1, (err, data) => {
                        _assertNoError(err, 'putting first object');
                        expect(data.VersionId).toBe(undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting master version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[0]);
                        callback();
                    }),
                    callback => s3.putObject(params2, (err, data) => {
                        _assertNoError(err, 'putting second object');
                        expect(data.VersionId).toBe(undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'getting null version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[1]);
                        callback();
                    }),
                ], done);
            });

            // Jira issue: S3C-444
            test('put object after put object acl on null version which is ' +
            'latest version should not result in two null version with ' +
            'different version ids', done => {
                async.waterfall([
                    // create new null version (master version in metadata)
                    callback => s3.putObject({ Bucket: bucket, Key: key },
                        err => callback(err)),
                    callback => checkOneVersion(s3, bucket, 'null', callback),
                    // note after put object acl in metadata will have null
                    // version (with same version ID) stored in both master and
                    // separate version due to using versionId=<null ver id>
                    // option in metadata PUT call
                    callback => s3.putObjectAcl({
                        Bucket: bucket,
                        Key: key,
                        ACL: 'public-read-write',
                        VersionId: 'null',
                    }, err => callback(err)),
                    // before overwriting master version, put object should
                    // clean up latest null version (both master version and
                    // separate version in metadata)
                    callback => s3.putObject({ Bucket: bucket, Key: key },
                        err => callback(err)),
                    // if clean-up did not occur, would see two null versions
                    // with different version IDs in version listing
                    callback => checkOneVersion(s3, bucket, 'null', callback),
                ], done);
            });

            // Jira issue: S3C-444
            test('put object after creating dual null version another way ' +
            'should not result in two null version with different version ids', done => {
                async.waterfall([
                    // create dual null version state another way
                    callback =>
                        createDualNullVersion(s3, bucket, key, callback),
                    // versioning is left enabled after above step
                    callback => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }, err => callback(err)),
                    // before overwriting master version, put object should
                    // clean up latest null version (both master version and
                    // separate version in metadata)
                    callback => s3.putObject({ Bucket: bucket, Key: key },
                        err => callback(err)),
                    // if clean-up did not occur, would see two null versions
                    // with different version IDs in version listing
                    callback => checkOneVersion(s3, bucket, 'null', callback),
                ], done);
            });
        });

        describe('on a version-suspended bucket with non-versioned object',
        () => {
            const eTags = [];

            beforeEach(done => {
                s3.putObject({ Bucket: bucket, Key: key, Body: data[0] },
                    (err, data) => {
                        if (err) {
                            done(err);
                        }
                        eTags.push(data.ETag);
                        s3.putBucketVersioning({
                            Bucket: bucket,
                            VersioningConfiguration: versioningSuspended,
                        }, done);
                    });
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            test('should get null version (latest) in versioning ' +
            'suspended bucket without specifying version id', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                };
                s3.getObject(paramsNull, (err, data) => {
                    expect(data.VersionId).toBe('null');
                    _assertNoError(err, 'getting null version');
                    done();
                });
            });

            test('should get null version in versioning suspended bucket ' +
            'specifying version id', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.getObject(paramsNull, (err, data) => {
                    expect(data.VersionId).toBe('null');
                    _assertNoError(err, 'getting null version');
                    done();
                });
            });

            test('should update null version in versioning suspended bucket', done => {
                const params = { Bucket: bucket, Key: key };
                const putParams = { Bucket: bucket, Key: key, Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                async.waterfall([
                    callback => s3.getObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'getting null version');
                        expect(data.VersionId).toBe('null');
                        callback();
                    }),
                    callback => s3.putObject(putParams, (err, data) => {
                        _assertNoError(err, 'putting object');
                        expect(data.VersionId).toBe(undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'getting null version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[1]);
                        callback();
                    }),
                    callback => s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting master version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[1]);
                        callback();
                    }),
                ], done);
            });
        });

        describe('on versioning suspended then enabled bucket w/ null version',
        () => {
            const eTags = [];
            beforeEach(done => {
                const params = { Bucket: bucket, Key: key, Body: data[0] };
                async.waterfall([
                    callback => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }, err => callback(err)),
                    callback => s3.putObject(params, (err, data) => {
                        if (err) {
                            callback(err);
                        }
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.putBucketVersioning({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }, callback),
                ], done);
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            test('should preserve the null version when creating new versions', done => {
                const params = { Bucket: bucket, Key: key };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                async.waterfall([
                    callback => s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        expect(nullVerData.ETag).toBe(eTags[0]);
                        expect(nullVerData.VersionId).toBe('null');
                        callback();
                    }),
                    callback => async.timesSeries(counter, (i, next) =>
                        s3.putObject(params, (err, data) => {
                            _assertNoError(err, `putting object #${i}`);
                            expect(data.VersionId).not.toEqual(undefined);
                            next();
                        }), err => callback(err)),
                    callback => s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        expect(nullVerData.ETag).toBe(eTags[0]);
                        callback();
                    }),
                ], done);
            });

            test('should create a bunch of objects and their versions', done => {
                const vids = [];
                const keycount = 50;
                const versioncount = 20;
                const value = '{"foo":"bar"}';
                async.times(keycount, (i, next1) => {
                    const key = `foo${i}`;
                    const params = { Bucket: bucket, Key: key, Body: value };
                    async.times(versioncount, (j, next2) =>
                        s3.putObject(params, (err, data) => {
                            expect(err).toBe(null);
                            expect(data.VersionId).toBeTruthy();
                            vids.push({ Key: key, VersionId: data.VersionId });
                            next2();
                        }), next1);
                }, err => {
                    expect(err).toBe(null);
                    expect(vids.length).toBe(keycount * versioncount);
                    done();
                });
            });
        });
    });
});
