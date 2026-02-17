const assert = require('assert');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteObjectCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectAttributesCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { removeAllVersions, versioningEnabled } = require('../../lib/utility/versioning-util.js');

const bucket = 'testbucket';
const key = 'testobject';
const body = 'hello world!';
const expectedMD5 = 'fc3ff98e8c6a0d3087d515c0473f8677';

describe('Test get object attributes with versioning', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: versioningEnabled,
            }));
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.send(new DeleteBucketCommand({ Bucket: bucket }))
                    .then(() => done())
                    .catch(done);
            });
        });

        it('should return NoSuchVersion for non-existent versionId', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));

            const fakeVersionId = '111111111111111111111111111111111111111175636f7270';

            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: fakeVersionId,
                    ObjectAttributes: ['ETag'],
                }));
                assert.fail('Expected NoSuchVersion error');
            } catch (err) {
                assert.strictEqual(err.name, 'NoSuchVersion');
                assert.strictEqual(
                    err.message,
                    'Indicates that the version ID specified in the request does not match an existing version.',
                );
            }
        });

        it('should return MethodNotAllowed for delete marker', async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));

            await s3.send(new DeleteObjectCommand({
                Bucket: bucket,
                Key: key,
            }));

            try {
                await s3.send(new GetObjectAttributesCommand({
                    Bucket: bucket,
                    Key: key,
                    ObjectAttributes: ['ETag'],
                }));
                assert.fail('Expected MethodNotAllowed error');
            } catch (err) {
                assert.strictEqual(err.name, 'MethodNotAllowed');
                assert.strictEqual(err.message, 'The specified method is not allowed against this resource.');
            }
        });

        it('should return attributes for specific version', async () => {
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));
            const versionId = putResult.VersionId;

            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                VersionId: versionId,
                ObjectAttributes: ['ETag', 'ObjectSize'],
            }));

            assert.strictEqual(data.ETag, expectedMD5);
            assert.strictEqual(data.ObjectSize, body.length);
            assert(data.LastModified, 'LastModified should be present');
        });

        it('should return VersionId for versioned object', async () => {
            const putResult = await s3.send(new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: body,
            }));
            const versionId = putResult.VersionId;

            const data = await s3.send(new GetObjectAttributesCommand({
                Bucket: bucket,
                Key: key,
                ObjectAttributes: ['ETag'],
            }));

            assert.strictEqual(data.VersionId, versionId);
        });
    });
});
