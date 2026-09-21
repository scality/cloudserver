const assert = require('assert');
const async = require('async');
const { promisify } = require('util');

const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    HeadObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const { removeAllVersions, versioningEnabled, versioningSuspended } = require('../../lib/utility/versioning-util.js');

const removeAllVersionsPromise = promisify(removeAllVersions);
const data = ['foo1', 'foo2'];
const counter = 100;
let bucket;
const key = '/';

function _assertNoError(err, desc) {
    assert.strictEqual(err, null, `Unexpected err ${desc}: ${err}`);
}

// Same tests as objectPut versioning tests, but head object instead of get
describe('put and head object with versioning', function testSuite() {
    this.timeout(600000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        beforeEach(async () => {
            bucket = `versioning-bucket-${Date.now()}`;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await removeAllVersionsPromise({ Bucket: bucket });
            await bucketUtil.empty(bucket, true);
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it(
            'should put and head a non-versioned object without including ' + 'version ids in response headers',
            done => {
                const params = { Bucket: bucket, Key: key };
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        _assertNoError(null, 'putting object');
                        assert.strictEqual(data.VersionId, undefined);
                        return s3.send(new HeadObjectCommand(params));
                    })
                    .then(data => {
                        _assertNoError(null, 'heading object');
                        assert.strictEqual(data.VersionId, undefined);
                        done();
                    })
                    .catch(done);
            },
        );

        it('version-specific head should still not return version id in ' + 'response header', done => {
            const params = { Bucket: bucket, Key: key };
            s3.send(new PutObjectCommand(params))
                .then(data => {
                    _assertNoError(null, 'putting object');
                    assert.strictEqual(data.VersionId, undefined);
                    params.VersionId = 'null';
                    return s3.send(new HeadObjectCommand(params));
                })
                .then(data => {
                    _assertNoError(null, 'heading specific version "null"');
                    assert.strictEqual(data.VersionId, undefined);
                    done();
                })
                .catch(done);
        });

        describe('on a version-enabled bucket', () => {
            beforeEach(async () => {
                await s3.send(
                    new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: versioningEnabled,
                    }),
                );
            });

            it('should create a new version for an object', done => {
                const params = { Bucket: bucket, Key: key };
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        _assertNoError(null, 'putting object');
                        params.VersionId = data.VersionId;
                        return s3.send(new HeadObjectCommand(params));
                    })
                    .then(data => {
                        _assertNoError(null, 'heading object');
                        assert.strictEqual(params.VersionId, data.VersionId, 'version ids are not equal');
                        done();
                    })
                    .catch(done);
            });
        });

        describe('on a version-enabled bucket w/ non-versioned object', () => {
            const eTags = [];

            beforeEach(done => {
                s3.send(
                    new PutObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        Body: data[0],
                    }),
                )
                    .then(data => {
                        eTags.push(data.ETag);
                        return s3.send(
                            new PutBucketVersioningCommand({
                                Bucket: bucket,
                                VersioningConfiguration: versioningEnabled,
                            }),
                        );
                    })
                    .then(() => done())
                    .catch(done);
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            it('should head null version in versioning enabled bucket', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                s3.send(new HeadObjectCommand(paramsNull))
                    .then(() => {
                        _assertNoError(null, 'heading null version');
                        done();
                    })
                    .catch(done);
            });

            it('should keep null version and create a new version', done => {
                const params = { Bucket: bucket, Key: key, Body: data[1] };
                let newVersion;
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        newVersion = data.VersionId;
                        eTags.push(data.ETag);
                        return s3.send(
                            new HeadObjectCommand({
                                Bucket: bucket,
                                Key: key,
                                VersionId: newVersion,
                            }),
                        );
                    })
                    .then(data => {
                        assert.strictEqual(data.VersionId, newVersion, 'version ids are not equal');
                        assert.strictEqual(data.ETag, eTags[1]);
                        return s3.send(
                            new HeadObjectCommand({
                                Bucket: bucket,
                                Key: key,
                                VersionId: 'null',
                            }),
                        );
                    })
                    .then(data => {
                        _assertNoError(null, 'heading null version');
                        assert.strictEqual(data.VersionId, 'null');
                        assert.strictEqual(data.ETag, eTags[0]);
                        done();
                    })
                    .catch(done);
            });

            it('should create new versions but still keep nullVersionId', done => {
                const versionIds = [];
                const params = { Bucket: bucket, Key: key };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                // create new versions
                async.timesSeries(
                    counter,
                    (i, next) => {
                        s3.send(new PutObjectCommand(params))
                            .then(data => {
                                versionIds.push(data.VersionId);
                                // head the 'null' version
                                return s3.send(new HeadObjectCommand(paramsNull));
                            })
                            .then(nullVerData => {
                                assert.strictEqual(nullVerData.ETag, eTags[0]);
                                assert.strictEqual(nullVerData.VersionId, 'null');
                                next();
                            })
                            .catch(next);
                    },
                    done,
                );
            });
        });

        describe('on version-suspended bucket', () => {
            beforeEach(async () => {
                await s3.send(
                    new PutBucketVersioningCommand({
                        Bucket: bucket,
                        VersioningConfiguration: versioningSuspended,
                    }),
                );
            });

            it('should not return version id for new object', done => {
                const params = { Bucket: bucket, Key: key, Body: 'foo' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                let eTag;
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        eTag = data.ETag;
                        _assertNoError(null, 'putting object');
                        assert.strictEqual(data.VersionId, undefined);
                        // heading null version should return object we just put
                        return s3.send(new HeadObjectCommand(paramsNull));
                    })
                    .then(nullVerData => {
                        _assertNoError(null, 'heading null version');
                        assert.strictEqual(nullVerData.ETag, eTag);
                        assert.strictEqual(nullVerData.VersionId, 'null');
                        done();
                    })
                    .catch(done);
            });

            it('should update null version if put object twice', done => {
                const params = { Bucket: bucket, Key: key };
                const params1 = { Bucket: bucket, Key: key, Body: data[0] };
                const params2 = { Bucket: bucket, Key: key, Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                const eTags = [];
                async.waterfall(
                    [
                        callback =>
                            s3
                                .send(new PutObjectCommand(params1))
                                .then(data => {
                                    _assertNoError(null, 'putting first object');
                                    assert.strictEqual(data.VersionId, undefined);
                                    eTags.push(data.ETag);
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new HeadObjectCommand(params))
                                .then(data => {
                                    _assertNoError(null, 'heading master version');
                                    assert.strictEqual(data.VersionId, 'null');
                                    assert.strictEqual(data.ETag, eTags[0], 'wrong object data');
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new PutObjectCommand(params2))
                                .then(data => {
                                    _assertNoError(null, 'putting second object');
                                    assert.strictEqual(data.VersionId, undefined);
                                    eTags.push(data.ETag);
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new HeadObjectCommand(paramsNull))
                                .then(data => {
                                    _assertNoError(null, 'heading null version');
                                    assert.strictEqual(data.VersionId, 'null');
                                    assert.strictEqual(data.ETag, eTags[1], 'wrong object data');
                                    callback();
                                })
                                .catch(callback),
                    ],
                    done,
                );
            });
        });

        describe('on a version-suspended bucket with non-versioned object', () => {
            const eTags = [];

            beforeEach(done => {
                s3.send(
                    new PutObjectCommand({
                        Bucket: bucket,
                        Key: key,
                        Body: data[0],
                    }),
                )
                    .then(data => {
                        eTags.push(data.ETag);
                        return s3.send(
                            new PutBucketVersioningCommand({
                                Bucket: bucket,
                                VersioningConfiguration: versioningSuspended,
                            }),
                        );
                    })
                    .then(() => done())
                    .catch(done);
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            it('should head null version in versioning suspended bucket', done => {
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                s3.send(new HeadObjectCommand(paramsNull))
                    .then(() => {
                        _assertNoError(null, 'heading null version');
                        done();
                    })
                    .catch(done);
            });

            it('should update null version in versioning suspended bucket', done => {
                const params = { Bucket: bucket, Key: key };
                const putParams = { Bucket: bucket, Key: '/', Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                async.waterfall(
                    [
                        callback =>
                            s3
                                .send(new HeadObjectCommand(paramsNull))
                                .then(data => {
                                    _assertNoError(null, 'heading null version');
                                    assert.strictEqual(data.VersionId, 'null');
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new PutObjectCommand(putParams))
                                .then(data => {
                                    _assertNoError(null, 'putting object');
                                    assert.strictEqual(data.VersionId, undefined);
                                    eTags.push(data.ETag);
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new HeadObjectCommand(paramsNull))
                                .then(data => {
                                    _assertNoError(null, 'heading null version');
                                    assert.strictEqual(data.VersionId, 'null');
                                    assert.strictEqual(data.ETag, eTags[1], 'wrong object data');
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(new HeadObjectCommand(params))
                                .then(data => {
                                    _assertNoError(null, 'heading master version');
                                    assert.strictEqual(data.VersionId, 'null');
                                    assert.strictEqual(data.ETag, eTags[1], 'wrong object data');
                                    callback();
                                })
                                .catch(callback),
                    ],
                    done,
                );
            });
        });

        describe('on versioning suspended then enabled bucket w/ null version', () => {
            const eTags = [];
            beforeEach(done => {
                const params = { Bucket: bucket, Key: key, Body: data[0] };
                async.waterfall(
                    [
                        callback =>
                            s3
                                .send(
                                    new PutBucketVersioningCommand({
                                        Bucket: bucket,
                                        VersioningConfiguration: versioningSuspended,
                                    }),
                                )
                                .then(() => callback())
                                .catch(callback),
                        callback =>
                            s3
                                .send(new PutObjectCommand(params))
                                .then(data => {
                                    eTags.push(data.ETag);
                                    callback();
                                })
                                .catch(callback),
                        callback =>
                            s3
                                .send(
                                    new PutBucketVersioningCommand({
                                        Bucket: bucket,
                                        VersioningConfiguration: versioningEnabled,
                                    }),
                                )
                                .then(() => callback())
                                .catch(callback),
                    ],
                    done,
                );
            });

            afterEach(done => {
                // reset eTags
                eTags.length = 0;
                done();
            });

            it('should preserve the null version when creating new versions', done => {
                const params = { Bucket: bucket, Key: key };
                const paramsNull = {
                    Bucket: bucket,
                    Key: '/',
                    VersionId: 'null',
                };
                async.waterfall(
                    [
                        cb =>
                            s3
                                .send(new HeadObjectCommand(paramsNull))
                                .then(nullVerData => {
                                    _assertNoError(null, 'heading null version');
                                    assert.strictEqual(nullVerData.ETag, eTags[0]);
                                    assert.strictEqual(nullVerData.VersionId, 'null');
                                    cb();
                                })
                                .catch(cb),
                        cb =>
                            async.timesSeries(
                                counter,
                                (i, next) =>
                                    s3
                                        .send(new PutObjectCommand(params))
                                        .then(data => {
                                            _assertNoError(null, `putting object #${i}`);
                                            assert.notEqual(data.VersionId, undefined);
                                            next();
                                        })
                                        .catch(next),
                                err => cb(err),
                            ),
                        cb =>
                            s3
                                .send(new HeadObjectCommand(paramsNull))
                                .then(nullVerData => {
                                    _assertNoError(null, 'heading null version');
                                    assert.strictEqual(nullVerData.ETag, eTags[0]);
                                    cb();
                                })
                                .catch(cb),
                    ],
                    done,
                );
            });

            it('should create a bunch of objects and their versions', done => {
                const vids = [];
                const keycount = 50;
                const versioncount = 20;
                const value = '{"foo":"bar"}';
                async.timesLimit(
                    keycount,
                    10,
                    (i, next1) => {
                        const key = `foo${i}`;
                        const params = { Bucket: bucket, Key: key, Body: value };
                        async.timesLimit(
                            versioncount,
                            10,
                            (j, next2) =>
                                s3
                                    .send(new PutObjectCommand(params))
                                    .then(data => {
                                        assert(data.VersionId, 'invalid versionId');
                                        vids.push({ Key: key, VersionId: data.VersionId });
                                        next2();
                                    })
                                    .catch(next2),
                            next1,
                        );
                    },
                    err => {
                        assert.strictEqual(err, null);
                        assert.strictEqual(vids.length, keycount * versioncount);
                        done();
                    },
                );
            });
        });
    });
});
