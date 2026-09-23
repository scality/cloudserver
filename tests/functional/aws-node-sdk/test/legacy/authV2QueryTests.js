const assert = require('assert');
const process = require('node:process');
const cp = require('child_process');
const util = require('util');
const timers = require('timers/promises');
const {
    S3Client,
    CreateBucketCommand,
    PutObjectCommand,
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
const almostOutsideTime = 99990;
const itSkipAWS = process.env.AWS_ON_AIR ? it.skip : it;

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

describe('aws-node-sdk v2auth query tests', function testSuite() {
    this.timeout(60000);
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v2' });
        s3 = new S3Client(config);
    });

    // AWS allows an expiry further in the future
    // 604810 seconds is higher that the Expires time limit: 604800 seconds
    // ( seven days)
    itSkipAWS('should return an error code if expires header is too far ' + 'in the future', async () => {
        // First, get a valid signed URL with maximum allowed expiry
        const command = new CreateBucketCommand({ Bucket: bucket });
        const validUrl = await getSignedUrl(s3, command, { expiresIn: 604800 }); // Exactly 7 days

        // Manually modify the URL to have a longer expiry
        const urlObj = new URL(validUrl);
        const futureExpiry = Math.floor(Date.now() / 1000) + 604810; // 10 seconds more than limit
        urlObj.searchParams.set('Expires', futureExpiry.toString());
        const invalidUrl = urlObj.toString();
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', invalidUrl]);
        assert.strictEqual(httpCode, '403 FORBIDDEN');
    });

    it('should return an error code if request occurs after expiry', async () => {
        const command = new CreateBucketCommand({ Bucket: bucket });
        const url = await getSignedUrl(s3, command, { expiresIn: 1 });
        await timers.setTimeout(1500);
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url]);
        assert.strictEqual(httpCode, '403 FORBIDDEN');
    });

    it('should create a bucket', async () => {
        const command = new CreateBucketCommand({ Bucket: bucket });
        const url = await getSignedUrl(s3, command, { expiresIn: almostOutsideTime });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url]);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should put an object', async () => {
        const command = new PutObjectCommand({ Bucket: bucket, Key: 'key' });
        const url = await getSignedUrl(s3, command, { expiresIn: almostOutsideTime });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url, '--upload-file', 'uploadFile']);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should put an object with an acl setting and a storage class setting', async () => {
        // This will test that upper case query parameters and lowercase
        // query parameters (i.e., 'x-amz-acl') are being sorted properly.
        // This will also test that query params that contain "x-amz-"
        // are being added to the canonical headers list in our string
        // to sign.
        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: 'key',
            ACL: 'public-read',
            StorageClass: 'STANDARD',
        });
        const url = await getSignedUrl(s3, command);
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'PUT', url, '--upload-file', 'uploadFile']);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('should get an object', async () => {
        const command = new GetObjectCommand({ Bucket: bucket, Key: 'key' });
        const url = await getSignedUrl(s3, command, { expiresIn: almostOutsideTime });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-o', 'download', url]);
        assert.strictEqual(httpCode, '200 OK');
    });

    it('downloaded file should equal file that was put', done => {
        diff('uploadFile', 'download', () => {
            deleteFile('download', done);
        });
    });

    it('should delete an object', async () => {
        const command = new DeleteObjectCommand({ Bucket: bucket, Key: 'key' });
        const url = await getSignedUrl(s3, command, { expiresIn: almostOutsideTime });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });

    it('should delete a bucket', async () => {
        const command = new DeleteBucketCommand({ Bucket: bucket });
        const url = await getSignedUrl(s3, command, { expiresIn: almostOutsideTime });
        const { httpCode } = await provideRawOutputAsync(['-verbose', '-X', 'DELETE', url]);
        assert.strictEqual(httpCode, '204 NO CONTENT');
    });
});
