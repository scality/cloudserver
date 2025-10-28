const assert = require('assert');
const async = require('async');

const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    ListObjectsCommand,
    DeleteObjectCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');

const getConfig = require('../support/config');

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

describe('aws-node-sdk test bucket versioning listing', function testSuite() {
    this.timeout(600000);
    let s3;
    const masterVersions = [];
    const allVersions = [];

    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    after(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
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

    it('should create a bunch of objects and their versions', done => {
        const keycount = 20;
        const versioncount = 20;
        const value = '{"foo":"bar"}';
        async.timesLimit(keycount, 10, (i, next1) => {
            const key = `foo${i}`;
            masterVersions.push(key);
            const params = { Bucket: bucket, Key: key, Body: value };
            async.timesLimit(versioncount, 10, (j, next2) =>
                s3.send(new PutObjectCommand(params))
                    .then(data => {
                        assert(data.VersionId, 'invalid versionId');
                        allVersions.push({ Key: key, VersionId: data.VersionId });
                        next2();
                    })
                    .catch(next2),
                next1);
        }, err => {
            assert.strictEqual(err, null);
            assert.strictEqual(allVersions.length, keycount * versioncount);
            done();
        });
    });

    it('should list all latest versions', async () => {
        const params = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        const data = await s3.send(new ListObjectsCommand(params));
        const keys = data.Contents.map(entry => entry.Key);
        assert.deepStrictEqual(keys.sort(), masterVersions.sort(),
                'not same keys');
    });

    it('should create some delete markers', done => {
        const keycount = 15;
        async.times(keycount, (i, next) => {
            const key = masterVersions[i];
            const params = { Bucket: bucket, Key: key };
            s3.send(new DeleteObjectCommand(params))
                .then(data => {
                    assert(data.VersionId, 'invalid versionId');
                    allVersions.push({ Key: key, VersionId: data.VersionId });
                    next();
                })
                .catch(next);
        }, done);
    });

    it('should list all latest versions', async () => {
        const params = { Bucket: bucket, MaxKeys: 1000, Delimiter: '/' };
        const data = await s3.send(new ListObjectsCommand(params));
        const keys = data.Contents.map(entry => entry.Key);
        assert.deepStrictEqual(keys.sort(), masterVersions.sort().slice(15),
                'not same keys');
    });

    it('should list all versions', done => {
        const versions = [];
        const params = { Bucket: bucket, MaxKeys: 15, Delimiter: '/' };
    
        async.retry(100, done => {
            s3.send(new ListObjectVersionsCommand(params))
                .then(data => {
                    if (data.Versions) {
                        data.Versions.forEach(version => versions.push({
                            Key: version.Key, VersionId: version.VersionId }));
                    }
                    if (data.DeleteMarkers) {
                        data.DeleteMarkers.forEach(version => versions.push({
                            Key: version.Key, VersionId: version.VersionId }));
                    }
                    if (data.IsTruncated) {
                        params.KeyMarker = data.NextKeyMarker;
                        params.VersionIdMarker = data.NextVersionIdMarker;
                        return done('not done yet');
                    }
                    return done();
                })
                .catch(err => {
                    done(err);
                });
        }, err => {
            if (err) {
                return done(err);
            }

            assert.deepStrictEqual(versions.sort(comp), allVersions.sort(comp),
                    'not same versions');
            
            const objectsToDelete = versions
                .filter(v => v && v.Key && v.VersionId)
                .map(v => ({
                    Key: String(v.Key),
                    VersionId: String(v.VersionId),
                }));

            const deleteParams = { 
                Bucket: bucket, 
                Delete: { 
                    Objects: objectsToDelete,
                } 
            };            
            return s3.send(new DeleteObjectsCommand(deleteParams))
                .then(() => {
                    done();
                })
                .catch(err => {
                    done(err);
                });
        });
    });
});

