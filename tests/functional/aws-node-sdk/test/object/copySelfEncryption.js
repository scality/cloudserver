const assert = require('assert');
const { CreateBucketCommand, PutObjectCommand, CopyObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'self-copy-encryption-bucket';
const otherBucket = 'self-copy-encryption-other-bucket';
const key = 'self-copy-encryption-object';
const body = 'self-copy must keep these bytes readable';

describe('Object self-copy with encryption', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(new CreateBucketCommand({ Bucket: otherBucket }));
        });

        afterEach(async () => {
            const buckets = [bucket, otherBucket];
            await bucketUtil.emptyMany(buckets);
            await bucketUtil.deleteMany(buckets);
        });

        // Puts the object encrypted with aws:kms and returns the key S3 created for its bucket.
        async function putEncrypted(Bucket, Key) {
            const res = await s3.send(
                new PutObjectCommand({ Bucket, Key, Body: body, ServerSideEncryption: 'aws:kms' }),
            );
            return res.SSEKMSKeyId;
        }

        async function selfCopy(params) {
            return s3.send(
                new CopyObjectCommand({ Bucket: bucket, Key: key, CopySource: `${bucket}/${key}`, ...params }),
            );
        }

        async function assertReadable(expectedKeyId) {
            const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(await res.Body.transformToString(), body);
            assert.strictEqual(res.SSEKMSKeyId, expectedKeyId);
        }

        it('should return the body and the AES256 header after encrypting an unencrypted object', async () => {
            await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
            const res = await selfCopy({ ServerSideEncryption: 'AES256' });
            assert.strictEqual(res.ServerSideEncryption, 'AES256');
            const get = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
            assert.strictEqual(await get.Body.transformToString(), body);
            assert.strictEqual(get.ServerSideEncryption, 'AES256');
        });

        it('should keep the object readable when re-encrypting it with another key', async () => {
            const sourceKeyId = await putEncrypted(bucket, key);
            const otherKeyId = await putEncrypted(otherBucket, key);
            assert.notStrictEqual(otherKeyId, sourceKeyId);
            await selfCopy({ ServerSideEncryption: 'aws:kms', SSEKMSKeyId: otherKeyId });
            await assertReadable(otherKeyId);
        });

        it('should keep the object readable when replacing its metadata under the same key', async () => {
            const sourceKeyId = await putEncrypted(bucket, key);
            await selfCopy({
                ServerSideEncryption: 'aws:kms',
                SSEKMSKeyId: sourceKeyId,
                MetadataDirective: 'REPLACE',
                Metadata: { updated: 'true' },
            });
            await assertReadable(sourceKeyId);
        });
    });
});
