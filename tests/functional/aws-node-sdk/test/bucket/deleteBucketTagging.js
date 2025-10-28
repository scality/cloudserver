const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketTaggingCommand,
    GetBucketTaggingCommand,
    DeleteBucketTaggingCommand } = require('@aws-sdk/client-s3');

const assertError = require('../../../../utilities/bucketTagging-util');

const getConfig = require('../support/config');

const bucket = 'policyputtaggingtestbucket';

const validTagging = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
        {
            Key: 'key2',
            Value: 'value2',
        },
    ],
};

describe('aws-sdk test delete bucket tagging', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        s3.AccountId = '123456789012';
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should delete tag', async () => {
        await s3.send(new PutBucketTaggingCommand({
            AccountId: s3.AccountId,
            Tagging: validTagging, 
            Bucket: bucket,
        }));
        const res = await s3.send(new GetBucketTaggingCommand({
            AccountId: s3.AccountId,
            Bucket: bucket,
        }));
        assert.deepStrictEqual(res.TagSet, validTagging.TagSet);
        await s3.send(new DeleteBucketTaggingCommand({
            AccountId: s3.AccountId,
            Bucket: bucket,
        }));
        try {
            await s3.send(new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }));
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
    });

    it('should make no change when deleting tags on bucket with no tags', async () => {
        try {
            await s3.send(new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }));
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
        await s3.send(new DeleteBucketTaggingCommand({
            AccountId: s3.AccountId,
            Bucket: bucket,
        }));
        try {
            await s3.send(new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }));
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
    });
});
