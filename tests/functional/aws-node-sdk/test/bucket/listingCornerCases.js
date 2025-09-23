const { S3Client,
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
    ListObjectsCommand,
    ListObjectsV2Command,
    PutBucketVersioningCommand } = require('@aws-sdk/client-s3');
const async = require('async');
const assert = require('assert');

const getConfig = require('../support/config');

function cutAttributes(data) {
    const newContent = [];
    const newPrefixes = [];
    if (data.Contents) {
        data.Contents.forEach(item => {
            newContent.push(item.Key);
        });
        /* eslint-disable no-param-reassign */
        data.Contents = newContent;
    }
    if (data.CommonPrefixes) {
        data.CommonPrefixes.forEach(item => {
            newPrefixes.push(item.Prefix);
        });
        /* eslint-disable no-param-reassign */
        data.CommonPrefixes = newPrefixes;
    }
    if (data.NextMarker === '') {
        /* eslint-disable no-param-reassign */
        delete data.NextMarker;
    }
    if (data.EncodingType === '') {
        /* eslint-disable no-param-reassign */
        delete data.EncodingType;
    }
    if (data.Delimiter === '') {
        /* eslint-disable no-param-reassign */
        delete data.Delimiter;
    }
}

const Bucket = `bucket-listing-corner-cases-${Date.now()}`;

const objects = [
    { Bucket, Key: 'Pâtisserie=中文-español-English', Body: '' },
    { Bucket, Key: 'notes/spring/1.txt', Body: '' },
    { Bucket, Key: 'notes/spring/2.txt', Body: '' },
    { Bucket, Key: 'notes/spring/march/1.txt', Body: '' },
    { Bucket, Key: 'notes/summer/1.txt', Body: '' },
    { Bucket, Key: 'notes/summer/2.txt', Body: '' },
    { Bucket, Key: 'notes/summer/august/1.txt', Body: '' },
    { Bucket, Key: 'notes/year.txt', Body: '' },
    { Bucket, Key: 'notes/yore.rs', Body: '' },
    { Bucket, Key: 'notes/zaphod/Beeblebrox.txt', Body: '' },
];

describe('Listing corner cases tests', () => {
    let s3;
    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket }));
        await Promise.all(objects.map(o => s3.send(new PutObjectCommand(o))));
    });
    after(async () => {
        const data = await s3.send(new ListObjectsCommand({ Bucket }));
        await Promise.all(data.Contents.map(o => s3.send(new DeleteObjectCommand({ Bucket, Key: o.Key }))));
        await s3.send(new DeleteBucketCommand({ Bucket }));
    });
    it('should list everything', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({ Bucket }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Contents: [
                objects[0].Key,
                objects[1].Key,
                objects[2].Key,
                objects[3].Key,
                objects[4].Key,
                objects[5].Key,
                objects[6].Key,
                objects[7].Key,
                objects[8].Key,
                objects[9].Key,
            ],
            IsTruncated: false,
            Marker: '',
            MaxKeys: 1000,
            Name: Bucket,
            Prefix: ''
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with valid marker', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Marker: 'notes/summer/1.txt',
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Delimiter: '/',
            IsTruncated: false,
            Marker: 'notes/summer/1.txt',
            MaxKeys: 1000,
            Name: Bucket,
            Prefix: ''
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with unexpected marker', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Marker: 'zzzz',
        }));
        assert.deepStrictEqual(data, {
            IsTruncated: false,
            Marker: 'zzzz',
            Name: Bucket,
            Prefix: '',
            Delimiter: '/',
            MaxKeys: 1000,
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with unexpected marker and prefix', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Marker: 'notes/summer0',
            Prefix: 'notes/summer/',
        }));
        assert.deepStrictEqual(data, {
            IsTruncated: false,
            Marker: 'notes/summer0',
            Name: Bucket,
            Prefix: 'notes/summer/',
            Delimiter: '/',
            MaxKeys: 1000,
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with MaxKeys', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            MaxKeys: 3,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Contents: [objects[0].Key,
                objects[1].Key,
                objects[2].Key,
            ],
            IsTruncated: true,
            Marker: '',
            MaxKeys: 3, 
            Name: Bucket,
            Prefix: ''
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with big MaxKeys', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            MaxKeys: 15000,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Contents: [objects[0].Key,
                objects[1].Key,
                objects[2].Key,
                objects[3].Key,
                objects[4].Key,
                objects[5].Key,
                objects[6].Key,
                objects[7].Key,
                objects[8].Key,
                objects[9].Key,
            ],
            IsTruncated: false,
            Marker: '',
            MaxKeys: 15000,
            Name: Bucket,
            Prefix: ''
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with delimiter', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Contents: [objects[0].Key],
            CommonPrefixes: ['notes/'],
            Delimiter: '/',
            IsTruncated: false,
            Marker: '',
            MaxKeys: 1000,
            Name: Bucket,
            Prefix: ''
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with long delimiter', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: 'notes/summer',
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: '',
            IsTruncated: false,
            Contents: [objects[0].Key,
                objects[1].Key,
                objects[2].Key,
                objects[3].Key,
                objects[7].Key,
                objects[8].Key,
                objects[9].Key,
            ],
            Name: Bucket,
            Prefix: '',
            Delimiter: 'notes/summer',
            MaxKeys: 1000,
            CommonPrefixes: ['notes/summer'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with delimiter and prefix related to #147', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: '',
            IsTruncated: false,
            Contents: [
                objects[7].Key,
                objects[8].Key,
            ],
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1000,
            CommonPrefixes: [
                'notes/spring/',
                'notes/summer/',
                'notes/zaphod/',
            ],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with prefix and marker related to #147', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/year.txt',
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/year.txt',
            IsTruncated: false,
            Contents: [objects[8].Key],
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1000,
            CommonPrefixes: ['notes/zaphod/'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with all parameters 1 of 5', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/',
            NextMarker: 'notes/spring/',
            IsTruncated: true,
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
            CommonPrefixes: ['notes/spring/'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with all parameters 2 of 5', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/spring/',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/spring/',
            NextMarker: 'notes/summer/',
            IsTruncated: true,
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
            CommonPrefixes: ['notes/summer/'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with all parameters 3 of 5', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/summer/',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        // eslint-disable-next-line no-console
        console.log('data', data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/summer/',
            NextMarker: 'notes/year.txt',
            IsTruncated: true,
            Contents: ['notes/year.txt'],
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with all parameters 4 of 5', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/year.txt',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/year.txt',
            NextMarker: 'notes/yore.rs',
            IsTruncated: true,
            Contents: ['notes/yore.rs'],
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should list with all parameters 5 of 5', async () => {
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/yore.rs',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            Marker: 'notes/yore.rs',
            IsTruncated: false,
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
            CommonPrefixes: ['notes/zaphod/'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
    it('should end listing on last common prefix', async () => {
        await s3.send(new PutObjectCommand({
            Bucket,
            Key: 'notes/zaphod/TheFourth.txt',
            Body: '',
        }));
        const { $metadata, ...data } = await s3.send(new ListObjectsCommand({
            Bucket,
            Delimiter: '/',
            Prefix: 'notes/',
            Marker: 'notes/yore.rs',
            MaxKeys: 1,
        }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            IsTruncated: false,
            Marker: 'notes/yore.rs',
            Name: Bucket,
            Prefix: 'notes/',
            Delimiter: '/',
            MaxKeys: 1,
            CommonPrefixes: ['notes/zaphod/'],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });

    it('should not list DeleteMarkers for version suspended buckets', done => {
        const obj = { name: 'testDeleteMarker.txt', value: 'foo' };
        const bucketName = `bucket-test-delete-markers-not-listed${Date.now()}`;
        return async.waterfall([
            next => s3.send(new CreateBucketCommand({ Bucket: bucketName }))
                .then(() => next())
                .catch(next),
            next => {
                const params = {
                    Bucket: bucketName,
                    VersioningConfiguration: {
                        Status: 'Suspended',
                    },
                };
                return s3.send(new PutBucketVersioningCommand(params))
                    .then(() => next())
                    .catch(next);
            },
            next => s3.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: obj.name,
                    Body: obj.value,
                }))
                .then(() => next())
                .catch(next),
            next => s3.send(new ListObjectsV2Command({ Bucket: bucketName }))
                .then(res => {
                    assert.strictEqual(res.Contents.some(c => c.Key === obj.name), true);
                    next();
                })
                .catch(next),
            next => s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: obj.name,
                }))
                .then(res => {
                    const headers = res.DeleteMarker;
                    assert.strictEqual(
                        headers, true);
                    return next();
                })
                .catch(next),
            next => s3.send(new ListObjectsV2Command({ Bucket: bucketName }))
                .then(res => {
                    assert.strictEqual(res.Contents, undefined);
                    next();
                })
                .catch(next),
            next => s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: obj.name, VersionId: 'null' }))
                .then(() => next())
                .catch(next),
            next => s3.send(new DeleteBucketCommand({ Bucket: bucketName }))
                .then(() => next())
                .catch(next)
        ], err => done(err));
    });
});
