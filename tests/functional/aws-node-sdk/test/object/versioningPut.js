import assert from 'assert';
import { S3 } from 'aws-sdk';
import async from 'async';

import getConfig from '../support/config';

const bucket = `versioning-bucket-${Date.now()}`;

describe('aws-node-sdk test object versioning', function testSuite() {
    this.timeout(600000);
    let s3 = undefined;
    const versionIds = [];
    const counter = 100;

    // setup test
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    it('should create a non-versioned object', done => {
        const params = { Bucket: bucket, Key: '/' };
        s3.putObject(params, err => {
            assert.strictEqual(err, null);
            s3.getObject(params, err => {
                assert.strictEqual(err, null);
                done();
            });
        });
    });

    it('should accept valid versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, done);
    });

    it('should retrieve the valid versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, { Status: 'Enabled' });
            done();
        });
    });

    it('should create a new version for an object', done => {
        const params = { Bucket: bucket, Key: '/' };
        s3.putObject(params, (err, data) => {
            assert.strictEqual(err, null);
            params.VersionId = data.VersionId;
            versionIds.push(data.VersionId);
            s3.getObject(params, (err, data) => {
                assert.strictEqual(err, null);
                assert.strictEqual(params.VersionId, data.VersionId,
                        'version ids are not equal');
                // TODO compare the value of null version and the original
                // version when find out how to include value in the put
                params.VersionId = 'null';
                s3.getObject(params, done);
            });
        });
    });

    it('should create new versions but still keep nullVersionId', done => {
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };
        let nullVersionId = undefined;
        // create new versions
        async.timesSeries(counter, (i, next) => s3.putObject(params,
            (err, data) => {
                versionIds.push(data.VersionId);
                // get the 'null' version
                s3.getObject(paramsNull, (err, data) => {
                    assert.strictEqual(err, null);
                    if (nullVersionId === undefined) {
                        nullVersionId = data.VersionId;
                    }
                    // what to expect: nullVersionId should be the same
                    assert(nullVersionId, 'nullVersionId should be valid');
                    assert.strictEqual(nullVersionId, data.VersionId);
                    next(err);
                });
            }), done);
    });

    it('should accept valid versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Suspended',
            },
        };
        s3.putBucketVersioning(params, done);
    });

    it('should retrieve the valid versioning configuration', done => {
        const params = { Bucket: bucket };
        // s3.getBucketVersioning(params, done);
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, { Status: 'Suspended' });
            done();
        });
    });

    it('should update null version in versioning suspended bucket', done => {
        const params = { Bucket: bucket, Key: '/' };
        const paramsNull = { Bucket: bucket, Key: '/', VersionId: 'null' };
        let newNullVersionId = undefined;
        async.waterfall([
            callback => s3.getObject(paramsNull, err => {
                assert.strictEqual(err, null);
                callback();
            }),
            callback => s3.putObject(params, (err, data) => {
                assert.strictEqual(err, null);
                versionIds.push('null');
                newNullVersionId = data.VersionId;
                callback();
            }),
            callback => s3.getObject(paramsNull, (err, data) => {
                assert.strictEqual(err, null);
                assert.strictEqual(newNullVersionId, data.VersionId,
                        'version ids are not equal');
                callback();
            }),
            callback => s3.getObject(params, (err, data) => {
                assert.strictEqual(err, null);
                assert.strictEqual(newNullVersionId, data.VersionId,
                        'version ids are not equal');
                callback();
            }),
        ], done);
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
        let nullVersionId = undefined;
        async.waterfall([
            callback => s3.getObject(paramsNull, (err, data) => {
                assert.strictEqual(err, null);
                nullVersionId = data.VersionId;
                callback();
            }),
            callback => s3.putBucketVersioning(paramsVersioning,
                err => callback(err)),
            callback => async.timesSeries(counter, (i, next) =>
                s3.putObject(params, (err, data) => {
                    assert.strictEqual(err, null);
                    versionIds.push(data.VersionId);
                    next();
                }), err => callback(err)),
            callback => s3.getObject(paramsNull, (err, data) => {
                assert.strictEqual(err, null);
                assert.strictEqual(nullVersionId, data.VersionId,
                        'version ids are not equal');
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
            // TODO use delete marker and check with the result
            process.stdout.write('creating objects done, now deleting...');
            done();
        });
    });
});
