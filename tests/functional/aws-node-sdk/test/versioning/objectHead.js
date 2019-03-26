const assert = require('assert');
const async = require('async');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
} = require('../../lib/utility/versioning-util.js');

const data = ['foo1', 'foo2'];
const counter = 100;
let bucket;
const key = '/';

function _assertNoError(err, desc) {
    expect(err).toBe(null);
}


// Same tests as objectPut versioning tests, but head object instead of get
describe('put and head object with versioning', () => {
    this.timeout(600000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

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

        test('should put and head a non-versioned object without including ' +
        'version ids in response headers', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                expect(data.VersionId).toBe(undefined);
                s3.headObject(params, (err, data) => {
                    _assertNoError(err, 'heading object');
                    expect(data.VersionId).toBe(undefined);
                    done();
                });
            });
        });

        test('version-specific head should still not return version id in ' +
        'response header', done => {
            const params = { Bucket: bucket, Key: key };
            s3.putObject(params, (err, data) => {
                _assertNoError(err, 'putting object');
                expect(data.VersionId).toBe(undefined);
                params.VersionId = 'null';
                s3.headObject(params, (err, data) => {
                    _assertNoError(err, 'heading specific version "null"');
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
                    s3.headObject(params, (err, data) => {
                        _assertNoError(err, 'heading object');
                        expect(params.VersionId).toBe(data.VersionId);
                        done();
                    });
                });
            });
        });

        describe('on a version-enabled bucket w/ non-versioned object', () => {
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

            test('should head null version in versioning enabled bucket', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/', VersionId:
                    'null',
                };
                s3.headObject(paramsNull, err => {
                    _assertNoError(err, 'heading null version');
                    done();
                });
            });

            test('should keep null version and create a new version', done => {
                const params = { Bucket: bucket, Key: key, Body: data[1] };
                s3.putObject(params, (err, data) => {
                    const newVersion = data.VersionId;
                    eTags.push(data.ETag);
                    s3.headObject({ Bucket: bucket, Key: key,
                        VersionId: newVersion }, (err, data) => {
                        expect(err).toBe(null);
                        expect(data.VersionId).toBe(newVersion);
                        expect(data.ETag).toBe(eTags[1]);
                        s3.headObject({ Bucket: bucket, Key: key,
                            VersionId: 'null' }, (err, data) => {
                            _assertNoError(err, 'heading null version');
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
                    Bucket: bucket,
                    Key: '/', VersionId:
                    'null',
                };
                // create new versions
                async.timesSeries(counter, (i, next) => s3.putObject(params,
                    (err, data) => {
                        versionIds.push(data.VersionId);
                        // head the 'null' version
                        s3.headObject(paramsNull, (err, nullVerData) => {
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
                    Key: '/', VersionId:
                    'null',
                };
                s3.putObject(params, (err, data) => {
                    const eTag = data.ETag;
                    _assertNoError(err, 'putting object');
                    expect(data.VersionId).toBe(undefined);
                    // heading null version should return object we just put
                    s3.headObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'heading null version');
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
                    Key: '/', VersionId:
                    'null',
                };
                const eTags = [];
                async.waterfall([
                    callback => s3.putObject(params1, (err, data) => {
                        _assertNoError(err, 'putting first object');
                        expect(data.VersionId).toBe(undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.headObject(params, (err, data) => {
                        _assertNoError(err, 'heading master version');
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
                    callback => s3.headObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'heading null version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[1]);
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

            test('should head null version in versioning suspended bucket', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/', VersionId:
                    'null',
                };
                s3.headObject(paramsNull, err => {
                    _assertNoError(err, 'heading null version');
                    done();
                });
            });

            test('should update null version in versioning suspended bucket', done => {
                const params = { Bucket: bucket, Key: key };
                const putParams = { Bucket: bucket, Key: '/', Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/', VersionId:
                    'null',
                };
                async.waterfall([
                    callback => s3.headObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'heading null version');
                        expect(data.VersionId).toBe('null');
                        callback();
                    }),
                    callback => s3.putObject(putParams, (err, data) => {
                        _assertNoError(err, 'putting object');
                        expect(data.VersionId).toBe(undefined);
                        eTags.push(data.ETag);
                        callback();
                    }),
                    callback => s3.headObject(paramsNull, (err, data) => {
                        _assertNoError(err, 'heading null version');
                        expect(data.VersionId).toBe('null');
                        expect(data.ETag).toBe(eTags[1]);
                        callback();
                    }),
                    callback => s3.headObject(params, (err, data) => {
                        _assertNoError(err, 'heading master version');
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
                    Key: '/', VersionId:
                    'null',
                };
                async.waterfall([
                    cb => s3.headObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'heading null version');
                        expect(nullVerData.ETag).toBe(eTags[0]);
                        expect(nullVerData.VersionId).toBe('null');
                        cb();
                    }),
                    cb => async.timesSeries(counter, (i, next) =>
                        s3.putObject(params, (err, data) => {
                            _assertNoError(err, `putting object #${i}`);
                            expect(data.VersionId).not.toEqual(undefined);
                            next();
                        }), err => cb(err)),
                    cb => s3.headObject(paramsNull, (err, nullVerData) => {
                        _assertNoError(err, 'heading null version');
                        expect(nullVerData.ETag).toBe(eTags[0]);
                        cb();
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
