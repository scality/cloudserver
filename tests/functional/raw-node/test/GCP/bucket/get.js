const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const {
    ListObjectsCommand,
    HeadBucketCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
} = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { listingHardLimit } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const smallSize = 20;
const bigSize = listingHardLimit + 1;
const config = getRealAwsConfig(credentialOne);
const gcpClient = new GCP(config);

function populateBucket(createdObjects, callback) {
    process.stdout.write(
        `Putting ${createdObjects.length} objects into bucket\n`);
    async.mapLimit(
        createdObjects,
        10,
        (object, moveOn) => {
            const command = new PutObjectCommand({
                Bucket: bucketName,
                Key: object,
            });
            gcpClient.send(command)
                .then(() => moveOn())
                .catch(err => moveOn(err));
        },
        err => {
            if (err) {
                process.stdout
                    .write(`err putting objects ${err}\n`);
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
        (object, moveOn) => {
            const command = new DeleteObjectCommand({
                Bucket: bucketName,
                Key: object,
            });
            gcpClient.send(command)
                .then(() => moveOn())
                .catch(err => moveOn(err));
        },
        err => {
            if (err) {
                process.stdout
                    .write(`err deleting objects ${err}\n`);
            }
            return callback(err);
        }
    );
}

describe('GCP: GET Bucket', function testSuite() {
    this.timeout(180000);

    before(async () => {
        await gcpRetry(
            gcpClient,
            () => new CreateBucketCommand({
                Bucket: bucketName,
            }),
        );
    });

    after(async () => {
        await gcpRetry(
            gcpClient,
            () => new DeleteBucketCommand({
                Bucket: bucketName,
            }),
        );
    });

    describe('without existing bucket', () => {
        it('should return 404 and NoSuchBucket', async () => {
            const badBucketName = `nonexistingbucket-${genUniqID()}`;
            try {
                const command = new HeadBucketCommand({
                    Bucket: badBucketName,
                });
                await gcpClient.send(command);
                assert.fail('Expected NoSuchBucket error, but got success');
            } catch (err) {
                assert(err);
                const statusCode = err.$metadata && err.$metadata.httpStatusCode;
                assert.strictEqual(statusCode, 404);
                const errorName = err.name === 'NotFound' ? 'NoSuchBucket' : err.name;
                assert.strictEqual(errorName, 'NoSuchBucket');
            }
        });

        it('should return 200', async () => {
            const command = new ListObjectsCommand({
                Bucket: bucketName,
            });
            const res = await gcpClient.send(command);
            assert.strictEqual(res.$metadata?.httpStatusCode, 200);
        });
    });

    describe('with existing bucket', () => {
        describe('with less than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(smallSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));

            after(done => removeObjects(createdObjects, done));

            it(`should list all ${smallSize} created objects`, async () => {
                const command = new ListObjectsCommand({
                    Bucket: bucketName,
                });
                const res = await gcpClient.send(command);
                assert.strictEqual(res.Contents.length, smallSize);
            });

            describe('with MaxKeys at 10', () => {
                it('should list MaxKeys number of objects', async () => {
                    const command = new ListObjectsCommand({
                        Bucket: bucketName,
                        MaxKeys: 10,
                    });
                    const res = await gcpClient.send(command);
                    assert.strictEqual(res.Contents.length, 10);
                });
            });
        });

        describe('with more than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(bigSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));

            after(done => removeObjects(createdObjects, done));

            it('should list at max 1000 of objects created', async () => {
                const command = new ListObjectsCommand({
                    Bucket: bucketName,
                });
                const res = await gcpClient.send(command);
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
                    const command = new ListObjectsCommand({
                        Bucket: bucketName,
                        MaxKeys: 1001,
                    });
                    const res = await gcpClient.send(command);
                    assert.strictEqual(res.Contents.length, listingHardLimit);
                });
            });
        });
    });
});
