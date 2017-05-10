const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util');

const customS3Request = require('../../lib/utility/customS3Request');

const data = ['foo1', 'foo2'];
const counter = 100;
const key = 'objectKey';

function _assertNoError(err, desc) {
    assert.strictEqual(err, null, `Unexpected err ${desc}: ${err}`);
}


describe('put and get object with versioning', function testSuite() {
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

        it('should return InvalidArgument for a request with versionId query',
        done => {
            const params = { Bucket: bucket, Key: key };
            const query = { versionId: 'testVersionId' };
            customS3Request(s3.putObject, params, { query }, err => {
                assert(err, 'Expected error but did not find one');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        it('should return InvalidArgument for a request with empty string ' +
        'versionId query', done => {
            const params = { Bucket: bucket, Key: key };
            const query = { versionId: '' };
            customS3Request(s3.putObject, params, { query }, err => {
                assert(err, 'Expected error but did not find one');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                done();
            });
        });

        it('should put and get a non-versioned object without including ' +
        'version ids in response headers', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                assert.strictEqual(data.VersionId, undefined);
                s3.getObject(params, (err, data) => {
                    _assertNoError(err, 'getting object');
                    assert.strictEqual(data.VersionId, undefined);
                    done();
                });
            });
        });

        it('version-specific get should still not return version id in ' +
        'response header', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                assert.strictEqual(data.VersionId, undefined);
                params.VersionId = 'null';
                s3.getObject(params, (err, data) => {
                    _assertNoError(err, 'getting specific version "null"');
                    assert.strictEqual(data.VersionId, undefined);
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

            it('should create a new version for an object', done => {
                const params = { Bucket: bucket, Key: key };
                s3.putObject(params, (err, data) => {
                    _assertNoError(err, 'putting object');
                    params.VersionId = data.VersionId;
                    s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting object');
                        assert.strictEqual(params.VersionId, data.VersionId,
                                'version ids are not equal');
                        done();
                    });
                });
            });

            it('should create a new version with tag set for an object',
            done => {
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
                        assert.strictEqual(getTagParams.VersionId,
                          data.VersionId, 'version ids are not equal');
                        assert.strictEqual(data.TagSet[0].Key, tagKey);
                        assert.strictEqual(data.TagSet[0].Value, tagValue);
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

            it('should get null version in versioning enabled bucket',
            done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.getObject(paramsNull, err => {
                    _assertNoError(err, 'getting null version');
                    done();
                });
            });

            it('should keep null version and create a new version',
            done => {
                const params = { Bucket: bucket, Key: key, Body: data[1] };
                s3.putObject(params, (err, data) => {
                    const newVersion = data.VersionId;
                    eTags.push(data.ETag);
                    s3.getObject({ Bucket: bucket, Key: key,
                        VersionId: newVersion }, (err, data) => {
                        assert.strictEqual(err, null);
                        assert.strictEqual(data.VersionId, newVersion,
                            'version ids are not equal');
                        assert.strictEqual(data.ETag, eTags[1]);
                        s3.getObject({ Bucket: bucket, Key: key,
                        VersionId: 'null' }, (err, data) => {
                            _assertNoError(err, 'getting null version');
                            assert.strictEqual(data.VersionId, 'null');
                            assert.strictEqual(data.ETag, eTags[0]);
                            done();
                        });
                    });
                });
            });

            it('should create new versions but still keep nullVersionId',
            done => {
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
                            assert.strictEqual(err, null);
                            assert.strictEqual(nullVerData.ETag, eTags[0]);
                            assert.strictEqual(nullVerData.VersionId, 'null');
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

            it('should not return version id for new object', done => {
                const params = { Bucket: bucket, Key: key, Body: 'foo' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.putObject(params, (err, data) => {
                    const eTag = data.ETag;
                    _assertNoError(err, 'putting object');
                    assert.strictEqual(data.VersionId, undefined);
                    // getting null version should return object we just put
                    s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        assert.strictEqual(nullVerData.ETag, eTag);
                        assert.strictEqual(nullVerData.VersionId, 'null');
                        done();
                    });
                });
            });

            it('should update null version if put object twice', done => {
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
                        assert.strictEqual(data.VersionId, undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting master version');
                        assert.strictEqual(data.VersionId, 'null');
                        assert.strictEqual(data.ETag, eTags[0],
                            'wrong object data');
                        callback();
                    }),
                    callback => s3.putObject(params2, (err, data) => {
                        _assertNoError(err, 'putting second object');
                        assert.strictEqual(data.VersionId, undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'getting null version');
                        assert.strictEqual(data.VersionId, 'null');
                        assert.strictEqual(data.ETag, eTags[1],
                            'wrong object data');
                        callback();
                    }),
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

            it('should get null version in versioning suspended bucket',
            done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                s3.getObject(paramsNull, err => {
                    _assertNoError(err, 'getting null version');
                    done();
                });
            });

            it('should update null version in versioning suspended bucket',
            done => {
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
                        assert.strictEqual(data.VersionId, 'null');
                        callback();
                    }),
                    callback => s3.putObject(putParams, (err, data) => {
                        _assertNoError(err, 'putting object');
                        assert.strictEqual(data.VersionId, undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.getObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'getting null version');
                        assert.strictEqual(data.VersionId, 'null');
                        assert.strictEqual(data.ETag, eTags[1],
                            'wrong object data');
                        callback();
                    }),
                    callback => s3.getObject(params, (err, data) => {
                        _assertNoError(err, 'getting master version');
                        assert.strictEqual(data.VersionId, 'null');
                        assert.strictEqual(data.ETag, eTags[1],
                            'wrong object data');
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

            it('should preserve the null version when creating new versions',
            done => {
                const params = { Bucket: bucket, Key: key };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                async.waterfall([
                    callback => s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        assert.strictEqual(nullVerData.ETag, eTags[0]);
                        assert.strictEqual(nullVerData.VersionId, 'null');
                        callback();
                    }),
                    callback => async.timesSeries(counter, (i, next) =>
                        s3.putObject(params, (err, data) => {
                            _assertNoError(err, `putting object #${i}`);
                            assert.notEqual(data.VersionId, undefined);
                            next();
                        }), err => callback(err)),
                    callback => s3.getObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'getting null version');
                        assert.strictEqual(nullVerData.ETag, eTags[0]);
                        callback();
                    }),
                ], done);
            });

            it('should create a bunch of objects and their versions', done => {
                const vids = [];
                const keycount = 50;
                const versioncount = 20;
                const value = '{"foo":"bar"}';
                async.times(keycount, (i, next1) => {
                    const key = `foo${i}`;
                    const params = { Bucket: bucket, Key: key, Body: value };
                    async.times(versioncount, (j, next2) =>
                        s3.putObject(params, (err, data) => {
                            assert.strictEqual(err, null);
                            assert(data.VersionId, 'invalid versionId');
                            vids.push({ Key: key, VersionId: data.VersionId });
                            next2();
                        }), next1);
                }, err => {
                    assert.strictEqual(err, null);
                    assert.strictEqual(vids.length, keycount * versioncount);
                    done();
                });
            });
        });
    });
});
