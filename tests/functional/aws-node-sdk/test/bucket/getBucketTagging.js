const assertError = require('../../../../utilities/bucketTagging-util');
const { S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    GetBucketTaggingCommand,
    PutBucketTaggingCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');
const getConfig = require('../support/config');

const bucket = 'gettaggingtestbucket';

describe('aws-sdk test get bucket tagging', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        // Preserve AccountId property for tests
        s3.AccountId = '123456789012';
    });

    beforeEach(async () => {
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
    });

    afterEach(async () => {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should return accessDenied if expected bucket owner does not match', async () => {
        try {
            await s3.send(new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
                ExpectedBucketOwner: '944690102203',
            }));
            throw new Error('Expected AccessDenied error');
        } catch (err) {
            assertError(err, 'AccessDenied');
        }
    });

    it('should not return accessDenied if expected bucket owner matches', async () => {
        try {
            await s3.send(new GetBucketTaggingCommand({ 
                AccountId: s3.AccountId, 
                Bucket: bucket, 
                ExpectedBucketOwner: s3.AccountId 
            }));
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
    });

    it('should return the TagSet', async () => {
        const tagSet = {
            TagSet: [
                {
                    Key: 'key1',
                    Value: 'value1',
                },
            ],
        };
        await s3.send(new PutBucketTaggingCommand({
            AccountId: s3.AccountId,
            Tagging: tagSet,
            Bucket: bucket,
            ExpectedBucketOwner: s3.AccountId
        }));
        const result = await s3.send(new GetBucketTaggingCommand({
            AccountId: s3.AccountId,
            Bucket: bucket,
            ExpectedBucketOwner: s3.AccountId
        }));

        assert.deepStrictEqual(result.TagSet, tagSet.TagSet);
    });
});
