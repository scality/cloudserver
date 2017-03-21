import assert from 'assert';
import { S3 } from 'aws-sdk';
import async from 'async';

import getConfig from '../support/config';

const bucket = `versioning-bucket-${Date.now()}`;

function comp(v1, v2) {
    if (v1.Key > v2.Key) {
        return 1;
    }
    if (v1.Key < v2.Key) {
        return -1;
    }
    if (v1.VersionId > v2.VersionId) {
        return 1;
    }
    if (v1.VersionId < v2.VersionId) {
        return -1;
    }
    return 0;
}

const testing = process.env.VERSIONING === 'no' ? describe.skip : describe;

testing('aws-node-sdk test bucket versioning listing', function testSuite() {
    this.timeout(600000);
    let s3 = undefined;
    const masterVersions = [];
    const allVersions = [];

    // setup test
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should accept valid versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, done);
    });

    it('should create a bunch of objects and their versions', done => {
        const keycount = 20;
        const versioncount = 20;
        const value = '{"foo":"bar"}';
        async.times(keycount, (i, next1) => {
            const key = `foo${i}`;
            masterVersions.push(key);
            const params = { Bucket: bucket, Key: key, Body: value };
            async.times(versioncount, (j, next2) =>
                s3.putObject(params, (err, data) => {
                    assert.strictEqual(err, null);
                    assert(data.VersionId, 'invalid versionId');
                    allVersions.push({ Key: key, VersionId: data.VersionId });
                    next2();
                }), next1);
        }, err => {
            assert.strictEqual(err, null);
            assert.strictEqual(allVersions.length, keycount * versioncount);
            done();
        });
    });

    it('should list all latest versions', done => {
        const params = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        s3.listObjects(params, (err, data) => {
            const keys = data.Contents.map(entry => entry.Key);
            assert.deepStrictEqual(keys.sort(), masterVersions.sort(),
                    'not same keys');
            done();
        });
    });

    it('should create some delete markers', done => {
        const keycount = 15;
        async.times(keycount, (i, next) => {
            const key = masterVersions[i];
            const params = { Bucket: bucket, Key: key };
            s3.deleteObject(params, (err, data) => {
                assert.strictEqual(err, null);
                assert(data.VersionId, 'invalid versionId');
                allVersions.push({ Key: key, VersionId: data.VersionId });
                next();
            });
        }, done);
    });

    it('should list all latest versions', done => {
        const params = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        s3.listObjects(params, (err, data) => {
            const keys = data.Contents.map(entry => entry.Key);
            assert.deepStrictEqual(keys.sort(), masterVersions.sort().slice(15),
                    'not same keys');
            done();
        });
    });

    it('should list all versions', done => {
        const versions = [];
        const params = { Bucket: bucket, MaxKeys: 15, Delimiter: '/' };
        async.retry(100, done => s3.listObjectVersions(params, (err, data) => {
            data.Versions.forEach(version => versions.push({
                Key: version.Key, VersionId: version.VersionId }));
            data.DeleteMarkers.forEach(version => versions.push({
                Key: version.Key, VersionId: version.VersionId }));
            if (data.IsTruncated) {
                params.KeyMarker = data.NextKeyMarker;
                params.VersionIdMarker = data.NextVersionIdMarker;
                return done('not done yet');
            }
            return done();
        }), () => {
            assert.deepStrictEqual(versions.sort(comp), allVersions.sort(comp),
                    'not same versions');
            const params = { Bucket: bucket, Delete: { Objects: allVersions } };
            s3.deleteObjects(params, done);
        });
    });
});
