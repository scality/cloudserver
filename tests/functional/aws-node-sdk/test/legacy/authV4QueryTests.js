const assert = require('assert');
const process = require('node:process');
const cp = require('child_process');
const util = require('util');
const { parseString } = require('xml2js');
const parseStringAsync = util.promisify(parseString);

const {
    S3Client,
    ListBucketsCommand,
    CreateBucketCommand,
    PutObjectCommand,
    ListObjectsCommand,
    GetObjectCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');
const getConfig = require('../support/config');
const provideRawOutput = require('../../lib/utility/provideRawOutput');
const provideRawOutputAsync = util.promisify(provideRawOutput);

const random = Math.round(Math.random() * 100).toString();
const bucket = `mybucket-${random}`;

function diff(putFile, receivedFile, done) {
    process.stdout.write(`diff ${putFile} ${receivedFile}\n`);
    cp.spawn('diff', [putFile, receivedFile]).on('exit', code => {
        assert.strictEqual(code, 0);
        done();
    });
}

function deleteFile(file, callback) {
    process.stdout.write(`rm ${file}\n`);
    cp.spawn('rm', [file]).on('exit', () => {
        callback();
    });
}

describe('aws-node-sdk v4auth query tests', function testSuite() {
    this.timeout(60000);
    let s3;

    before(() => {
        const config = getConfig('default', {});
        s3 = new S3Client(config);
    });

    it('should do an empty bucket listing', async () => {
        const url = await getSignedUrl(s3, new ListBucketsCommand({}), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', url]);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should create a bucket', async () => {
        const params = { Bucket: bucket };
        const url = await getSignedUrl(
            s3,
            new CreateBucketCommand(params),
            { expiresIn: 900 }
        );
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url]);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should do a bucket listing with result', async () => {
        const url = await getSignedUrl(s3, new ListBucketsCommand({}), { expiresIn: 900 });
        const { httpCode, rawOutput } = await provideRawOutputAsync(['-verbose', url]);
        assert.strictEqual(httpCode, '200 OK');
        const xml = await parseStringAsync(rawOutput.stdout);
        const bucketNames = xml.ListAllMyBucketsResult
            .Buckets[0].Bucket.map(item => item.Name[0]);
        assert(bucketNames.indexOf(bucket) > -1);
    });

    it('should put an object', async () => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = await getSignedUrl(s3, new PutObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile']);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should put an object with an acl setting and a storage class setting', async () => {
        const params = {
            Bucket: bucket,
            Key: 'key',
            ACL: 'public-read',
            StorageClass: 'STANDARD',
            ContentType: 'text/plain',
        };
        const url = await getSignedUrl(s3, new PutObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile']);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should put an object with native characters', async () => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = await getSignedUrl(s3, new PutObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url,
            '--upload-file', 'uploadFile']);
        assert.strictEqual(httpCode, '200 OK');
    });

    // listObjects test
    it('should list objects in bucket', async () => {
        const params = { Bucket: bucket };
        const url = await getSignedUrl(s3, new ListObjectsCommand(params), { expiresIn: 900 });
        const { httpCode, rawOutput } = await provideRawOutputAsync(['-verbose', url]);
        assert.strictEqual(httpCode, '200 OK');
        const result = await parseStringAsync(rawOutput.stdout);
        assert.strictEqual(result.ListBucketResult.Contents[0].Key[0], 'key');
    });

    // getObject test
    it('should get an object', async () => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = await getSignedUrl(s3, new GetObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-o', 'download', url]);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('downloaded file should equal file that was put', done => {
        diff('uploadFile', 'download', () => {
            deleteFile('download', done);
        });
    });

    // deleteObject test
    it('should delete an object', async () => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = await getSignedUrl(s3, new DeleteObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });

    it('should return a 204 on delete of an already deleted object', async () => {
        const params = { Bucket: bucket, Key: 'key' };
        const url = await getSignedUrl(s3, new DeleteObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });

    it('should return 204 on delete of non-existing object', async () => {
        const params = { Bucket: bucket, Key: 'randomObject' };
        const url = await getSignedUrl(s3, new DeleteObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });

    it('should delete an object with native characters', async () => {
        const Key = 'key-pâtisserie-中文-español-English-हिन्दी-العربية-' +
        'português-বাংলা-русский-日本語-ਪੰਜਾਬੀ-한국어-தமிழ்';
        const params = { Bucket: bucket, Key };
        const url = await getSignedUrl(s3, new DeleteObjectCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });

    it('should delete a bucket', async () => {
        const params = { Bucket: bucket };
        const url = await getSignedUrl(s3, new DeleteBucketCommand(params), { expiresIn: 900 });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });
});
