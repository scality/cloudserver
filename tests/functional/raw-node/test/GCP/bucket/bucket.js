const assert = require('assert');
const async = require('async');
const util = require('util');
const arsenal = require('arsenal');
const {
    HeadBucketCommand,
    ListObjectsCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, genBucketName, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { listingHardLimit } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const config = getRealAwsConfig(credentialOne);
const gcpClient = new GCP(config);

describe('GCP: Bucket', function testSuite() {
    this.timeout(180000);

    const bucketName = genBucketName('bucket');

    before(async () => {
        process.stdout.write(`Creating test bucket ${bucketName}\n`);
        await gcpRetry(gcpClient, new CreateBucketCommand({ Bucket: bucketName }));
    });

    after(async () => {
        await gcpRetry(gcpClient, new DeleteBucketCommand({ Bucket: bucketName }));
    });

    describe('HEAD Bucket', () => {
        it('should return 404 for non-existing bucket', async () => {
            const badBucketName = `cldsrvci-bucket-${genUniqID()}`;
            try {
                await gcpClient.send(new HeadBucketCommand({ Bucket: badBucketName }));
                assert.fail('Expected 404 error, but got success');
            } catch (err) {
                assert(err);
                assert.strictEqual(err.$metadata?.httpStatusCode, 404);
                const errorName = err.name === 'NotFound' ? 'NoSuchBucket' : err.name;
                assert.strictEqual(errorName, 'NoSuchBucket');
            }
        });

        it('should return 200 and bucket metadata', async () => {
            // Need to use the helper headBucket function for middleware with MetaVersionId
            const res = await util.promisify(gcpClient.headBucket.bind(gcpClient))({ Bucket: bucketName });
            const { $metadata, ...data } = res;
            assert.strictEqual($metadata?.httpStatusCode, 200);
            // Ensure MetaVersionId is present and non-empty
            assert.ok(
                typeof data.MetaVersionId === 'string'
                && data.MetaVersionId.length > 0
            );
        });
    });

    describe('GET Bucket (List Objects)', () => {
        const smallSize = 20;
        const bigSize = listingHardLimit + 1;

        function populateBucket(createdObjects, callback) {
            process.stdout.write(
                `Putting ${createdObjects.length} objects into bucket\n`);
            async.mapLimit(
                createdObjects,
                10,
                async object => gcpClient.send(new PutObjectCommand({
                    Bucket: bucketName,
                    Key: object,
                })),
                err => {
                    if (err) {
                        process.stdout.write(`err putting objects ${err}\n`);
                    }
                    return callback(err);
                }
            );
        }

        function removeObjects(createdObjects, callback) {
            process.stdout.write(
                `Deleting ${createdObjects.length} objects from bucket\n`);
            async.mapLimit(
                createdObjects,
                10,
                async object => gcpClient.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: object,
                })),
                err => {
                    if (err) {
                        process.stdout.write(`err deleting objects ${err}\n`);
                    }
                    return callback(err);
                }
            );
        }

        it('should return 200', async () => {
            const res = await gcpClient.send(
                new ListObjectsCommand({ Bucket: bucketName }));
            assert.strictEqual(res.$metadata?.httpStatusCode, 200);
        });

        describe('with less than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(smallSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));
            after(done => removeObjects(createdObjects, done));

            it(`should list all ${smallSize} created objects`, async () => {
                const res = await gcpClient.send(
                    new ListObjectsCommand({ Bucket: bucketName }));
                assert.strictEqual(res.Contents.length, smallSize);
            });

            it('should list MaxKeys number of objects with MaxKeys at 10', async () => {
                const res = await gcpClient.send(new ListObjectsCommand({
                    Bucket: bucketName,
                    MaxKeys: 10,
                }));
                assert.strictEqual(res.Contents.length, 10);
            });
        });

        describe('with more than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(bigSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));
            after(done => removeObjects(createdObjects, done));

            it('should list at max 1000 of objects created', async () => {
                const res = await gcpClient.send(
                    new ListObjectsCommand({ Bucket: bucketName }));
                assert.strictEqual(res.Contents.length, listingHardLimit);
            });

            describe('with MaxKeys at 1001', () => {
                // TODO: S3C-5445
                // Note: this test is testing GCP behaviour, not the Cloudserver one.
                // It tests that GET https://<GCP_BUCKET_NAME>.storage.googleapis.com/?max-keys=1001
                // returns only the first 1000 objects.
                //
                // Expected behavior: the GCP XML API should not return a list longer
                // than 1000 objects, even if max-keys is greater than 1000:
                // https://cloud.google.com/storage/docs/xml-api/reference-headers#maxkeys
                //
                // Actual behavior: it returns a list longer than 1000 objects when
                // max-keys is greater than 1000
                it.skip('should list at max 1000, ignoring MaxKeys', async () => {
                    const res = await gcpClient.send(new ListObjectsCommand({
                        Bucket: bucketName,
                        MaxKeys: 1001,
                    }));
                    assert.strictEqual(res.Contents.length, listingHardLimit);
                });
            });
        });
    });
});
