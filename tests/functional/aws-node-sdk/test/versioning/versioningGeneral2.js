const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    GetBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
    DeleteObjectCommand,
    DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');
const async = require('async');

const getConfig = require('../support/config');

const bucket = `versioning-bucket-${Date.now()}`;

describe('aws-node-sdk test bucket versioning', function testSuite() {
    this.timeout(600000);
    let s3;
    const versionIds = [];
    const counter = 100;

    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    after(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should not accept empty versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {},
        };
        s3.send(new PutBucketVersioningCommand(params))
            .then(() => {
                done('accepted empty versioning configuration');
            })
            .catch(error => {
                assert.strictEqual(error.$metadata?.httpStatusCode, 400);
                assert.strictEqual(error.name, 'IllegalVersioningConfigurationException');
                done();
            });
    });

    it('should retrieve an empty versioning configuration', async () => {
        const params = { Bucket: bucket };
        const { $metadata, ...data } = await s3.send(new GetBucketVersioningCommand(params));
        assert.strictEqual($metadata?.httpStatusCode, 200);
        assert.deepStrictEqual(data, {});
    });

    it('should not accept versioning configuration w/o "Status"', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
            },
        };
        s3.send(new PutBucketVersioningCommand(params))
            .then(() => {
                done('accepted empty versioning configuration');
            })
            .catch(error => {
                assert.strictEqual(error.$metadata?.httpStatusCode, 400);
                assert.strictEqual(error.name, 'IllegalVersioningConfigurationException');
                done();
            });
    });

    it('should retrieve an empty versioning configuration', async () => {
        const params = { Bucket: bucket };
        const { $metadata, ...data } = await s3.send(new GetBucketVersioningCommand(params));
        assert.strictEqual($metadata?.httpStatusCode, 200);
        assert.deepStrictEqual(data, {});
    });

    it('should not accept versioning configuration w/ invalid value', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'fun',
                Status: "let's do it",
            },
        };
        s3.send(new PutBucketVersioningCommand(params))
            .then(() => {
                done('accepted empty versioning configuration');
            })
            .catch(error => {
                assert.strictEqual(error.$metadata?.httpStatusCode, 400);
                assert.strictEqual(error.name, 'IllegalVersioningConfigurationException');
                done();
            });
    });

    it('should retrieve an empty versioning configuration', async () => {
        const params = { Bucket: bucket };
        const { $metadata, ...data } = await s3.send(new GetBucketVersioningCommand(params));
        assert.strictEqual($metadata?.httpStatusCode, 200);
        assert.deepStrictEqual(data, {});
    });

    it('should create a non-versioned object', done => {
        const params = { Bucket: bucket, Key: '/' };
        s3.send(new PutObjectCommand(params))
            .then(() => s3.send(new GetObjectCommand(params)))
            .then(() => done())
            .catch(done);
    });

    it('should accept valid versioning configuration', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        await s3.send(new PutBucketVersioningCommand(params));
    });

    it('should retrieve the valid versioning configuration', async () => {
        const params = { Bucket: bucket };
        const data = await s3.send(new GetBucketVersioningCommand(params));
        assert.deepStrictEqual(data.Status, 'Enabled');
    });

    it('should create a new version for an object', done => {
        const params = { Bucket: bucket, Key: '/' };
        s3.send(new PutObjectCommand(params))
            .then(data => {
                params.VersionId = data.VersionId;
                versionIds.push(data.VersionId);
                return s3.send(new GetObjectCommand(params));
            })
            .then(data => {
                assert.strictEqual(params.VersionId, data.VersionId, 'version ids are not equal');
                // TODO compare the value of null version and the original
                // version when find out how to include value in the put
                params.VersionId = 'null';
                return s3.send(new GetObjectCommand(params));
            })
            .then(() => done())
            .catch(done);
    });

    it('should create new versions but still keep nullVersionId', done => {
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };
        let nullVersionId;
        // create new versions
        async.timesSeries(
            counter,
            (i, next) => {
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        versionIds.push(data.VersionId);
                        // get the 'null' version
                        return s3.send(new GetObjectCommand(paramsNull));
                    })
                    .then(data => {
                        if (nullVersionId === undefined) {
                            nullVersionId = data.VersionId;
                        }
                        // what to expect: nullVersionId should be the same
                        assert(nullVersionId, 'nullVersionId should be valid');
                        assert.strictEqual(nullVersionId, data.VersionId);
                        next();
                    })
                    .catch(next);
            },
            done,
        );
    });

    it('should accept valid versioning configuration', async () => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Suspended',
            },
        };
        await s3.send(new PutBucketVersioningCommand(params));
    });

    it('should retrieve the valid versioning configuration', async () => {
        const params = { Bucket: bucket };
        const data = await s3.send(new GetBucketVersioningCommand(params));
        assert.deepStrictEqual(data.Status, 'Suspended');
    });

    it('should update null version in versioning suspended bucket', done => {
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };

        async.waterfall(
            [
                callback =>
                    s3
                        .send(new GetObjectCommand(paramsNull))
                        .then(() => callback())
                        .catch(callback),
                callback =>
                    s3
                        .send(new PutObjectCommand(params))
                        .then(() => {
                            versionIds.push('null');
                            callback();
                        })
                        .catch(callback),
                callback =>
                    s3
                        .send(new GetObjectCommand(paramsNull))
                        .then(data => {
                            assert.strictEqual(data.VersionId, 'null', 'version ids are equal');
                            callback();
                        })
                        .catch(callback),
                callback =>
                    s3
                        .send(new GetObjectCommand(params))
                        .then(data => {
                            assert.strictEqual(data.VersionId, 'null', 'version ids are not equal');
                            callback();
                        })
                        .catch(callback),
            ],
            done,
        );
    });

    it('should enable versioning and preserve the null version', done => {
        const paramsVersioning = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };
        let nullVersionId;

        async.waterfall(
            [
                callback =>
                    s3
                        .send(new GetObjectCommand(paramsNull))
                        .then(data => {
                            nullVersionId = data.VersionId;
                            callback();
                        })
                        .catch(callback),
                callback =>
                    s3
                        .send(new PutBucketVersioningCommand(paramsVersioning))
                        .then(() => callback())
                        .catch(callback),
                callback =>
                    async.timesSeries(
                        counter,
                        (i, next) =>
                            s3
                                .send(new PutObjectCommand(params))
                                .then(data => {
                                    versionIds.push(data.VersionId);
                                    next();
                                })
                                .catch(next),
                        err => callback(err),
                    ),
                callback =>
                    s3
                        .send(new GetObjectCommand(paramsNull))
                        .then(data => {
                            assert.strictEqual(nullVersionId, data.VersionId, 'version ids are not equal');
                            callback();
                        })
                        .catch(callback),
            ],
            done,
        );
    });

    it('should create delete marker and keep the null version', done => {
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };

        s3.send(new GetObjectCommand(paramsNull))
            .then(data => {
                const nullVersionId = data.VersionId;
                async.timesSeries(
                    counter,
                    (i, next) => {
                        s3.send(new DeleteObjectCommand(params))
                            .then(data => {
                                versionIds.push(data.VersionId);
                                return s3.send(new GetObjectCommand(params));
                            })
                            .then(() => {
                                next(new Error('Expected NoSuchKey error'));
                            })
                            .catch(err => {
                                assert.strictEqual(err.name, 'NoSuchKey');
                                next();
                            });
                    },
                    err => {
                        if (err) {
                            return done(err);
                        }
                        return s3
                            .send(new GetObjectCommand(paramsNull))
                            .then(data => {
                                assert.strictEqual(nullVersionId, data.VersionId, 'version ids are not equal');
                                done();
                            })
                            .catch(done);
                    },
                );
            })
            .catch(done);
    });

    it('should delete latest version and get the next version', done => {
        versionIds.reverse();
        const params = { Bucket: bucket, Key: '/' };

        async.timesSeries(
            versionIds.length,
            (i, next) => {
                const versionId = versionIds[i];
                const nextVersionId = i < versionIds.length - 1 ? versionIds[i + 1] : undefined;
                const paramsVersion = { Bucket: bucket, Key: '/', VersionId: versionId };

                s3.send(new DeleteObjectCommand(paramsVersion))
                    .then(() => s3.send(new GetObjectCommand(params)))
                    .then(data => {
                        assert(data.VersionId, 'invalid versionId');
                        if (nextVersionId !== 'null') {
                            assert.strictEqual(data.VersionId, nextVersionId);
                        }
                        next();
                    })
                    .catch(err => {
                        assert(err.name === 'NotFound' || err.name === 'NoSuchKey', 'error');
                        next();
                    });
            },
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
                if (err) {
                    return done(err);
                }
                assert.strictEqual(vids.length, keycount * versioncount);
                const params = {
                    Bucket: bucket,
                    Delete: {
                        Objects: vids.map(v => ({
                            Key: v.Key,
                            VersionId: v.VersionId,
                        })),
                    },
                };
                // TODO use delete marker and check with the result
                process.stdout.write('creating objects done, now deleting...');
                return s3
                    .send(new DeleteObjectsCommand(params))
                    .then(() => done())
                    .catch(done);
            },
        );
    });
});
