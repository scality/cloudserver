const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    ListObjectsCommand,
    DeleteObjectCommand } = require('@aws-sdk/client-s3');
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

const Bucket = `bucket-skip-scan-${Date.now()}`;

describe('Skip scan cases tests', () => {
    let s3;
    before(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        
        // Create bucket
        await s3.send(new CreateBucketCommand({ Bucket }));
        
        /* generating different prefixes every x > STREAK_LENGTH
           to force the metadata backends to skip */
        const x = 120;
        
        // Create 500 objects with concurrency limit of 10
        const promises = [];
        for (let n = 0; n < 500; n++) {
            const putObjectPromise = async () => {
                const o = {};
                o.Bucket = Bucket;
                // eslint-disable-next-line
                o.Key = String.fromCharCode(65 + n / x) +
                    '/' + n % x;
                o.Body = '';
                await s3.send(new PutObjectCommand(o));
            };
            promises.push(putObjectPromise);
        }
        
        // Execute promises with concurrency limit of 10
        for (let i = 0; i < promises.length; i += 10) {
            const batch = promises.slice(i, i + 10);
            await Promise.all(batch.map(fn => fn()));
        }
    });
    
    after(async () => {
        // List all objects
        const data = await s3.send(new ListObjectsCommand({ Bucket }));
        
        // Delete all objects
        const deletePromises = data.Contents.map(o => 
            s3.send(new DeleteObjectCommand({ Bucket, Key: o.Key }))
        );
        await Promise.all(deletePromises);
        
        // Delete bucket
        await s3.send(new DeleteBucketCommand({ Bucket }));
    });
    
    it('should find all common prefixes in one shot', async () => {
        const { $metadata , ...data } = await s3.send(new ListObjectsCommand({ Bucket, Delimiter: '/' }));
        cutAttributes(data);
        assert.deepStrictEqual(data, {
            IsTruncated: false,
            Marker: '',
            Delimiter: '/',
            Name: Bucket,
            Prefix: '',
            MaxKeys: 1000,
            CommonPrefixes: [
                'A/',
                'B/',
                'C/',
                'D/',
                'E/',
            ],
        });
        assert.strictEqual($metadata.httpStatusCode, 200);
    });
});
