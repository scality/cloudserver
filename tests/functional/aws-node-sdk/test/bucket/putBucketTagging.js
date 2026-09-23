const assert = require('assert');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketTaggingCommand,
    GetBucketTaggingCommand,
} = require('@aws-sdk/client-s3');
const assertError = require('../../../../utilities/bucketTagging-util');

const getConfig = require('../support/config');

const bucket = 'policyputtaggingtestbucket';

const taggingNotUnique = {
    TagSet: [
        {
            Key: 'string',
            Value: 'string',
        },
        {
            Key: 'string',
            Value: 'stringaaaa',
        },
    ],
};

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

const validSingleTagging = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
    ],
};

const validEmptyTagging = {
    TagSet: [],
};

const taggingKeyNotValid = {
    TagSet: [
        {
            Key:
                'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'astringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            Value: 'string',
        },
        {
            Key: 'string',
            Value: 'stringaaaa',
        },
    ],
};

const taggingValueNotValid = {
    TagSet: [
        {
            Key: 'stringaaa',
            Value: 'string',
        },
        {
            Key: 'string',
            Value:
                'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        },
    ],
};

describe('aws-sdk test put bucket tagging', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        s3.AccountId = '123456789012';
    });

    beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucket })));

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: bucket })));

    it('should not add tag if tagKey not unique', async () => {
        try {
            await s3.send(
                new PutBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Tagging: taggingNotUnique,
                    Bucket: bucket,
                }),
            );
            throw new Error('Expected InvalidTag error');
        } catch (err) {
            assertError(err, 'InvalidTag');
        }
    });

    it('should not add tag if tagKey not valid', async () => {
        try {
            await s3.send(
                new PutBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Tagging: taggingKeyNotValid,
                    Bucket: bucket,
                }),
            );
            throw new Error('Expected InvalidTag error');
        } catch (err) {
            assertError(err, 'InvalidTag');
        }
    });

    it('should not add tag if tagValue not valid', async () => {
        try {
            await s3.send(
                new PutBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Tagging: taggingValueNotValid,
                    Bucket: bucket,
                }),
            );
            throw new Error('Expected InvalidTag error');
        } catch (err) {
            assertError(err, 'InvalidTag');
        }
    });

    it('should add tag', async () => {
        // Put bucket tagging
        await s3.send(
            new PutBucketTaggingCommand({
                AccountId: s3.AccountId,
                Tagging: validTagging,
                Bucket: bucket,
            }),
        );
        const res = await s3.send(
            new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }),
        );
        assert.deepStrictEqual(res.TagSet, validTagging.TagSet);
    });

    it('should be able to put single tag', async () => {
        await s3.send(
            new PutBucketTaggingCommand({
                AccountId: s3.AccountId,
                Tagging: validSingleTagging,
                Bucket: bucket,
            }),
        );
        const res = await s3.send(
            new GetBucketTaggingCommand({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }),
        );
        assert.deepStrictEqual(res.TagSet, validSingleTagging.TagSet);
    });

    it('should be able to put empty tag array', async () => {
        await s3.send(
            new PutBucketTaggingCommand({
                AccountId: s3.AccountId,
                Tagging: validEmptyTagging,
                Bucket: bucket,
            }),
        );
        try {
            await s3.send(
                new GetBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Bucket: bucket,
                }),
            );
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
    });

    it('should return accessDenied if expected bucket owner does not match', async () => {
        try {
            await s3.send(
                new PutBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Tagging: validEmptyTagging,
                    Bucket: bucket,
                    ExpectedBucketOwner: '944690102203',
                }),
            );
            throw new Error('Expected AccessDenied error');
        } catch (err) {
            assertError(err, 'AccessDenied');
        }
    });

    it('should not return accessDenied if expected bucket owner matches', async () => {
        await s3.send(
            new PutBucketTaggingCommand({
                AccountId: s3.AccountId,
                Tagging: validEmptyTagging,
                Bucket: bucket,
                ExpectedBucketOwner: s3.AccountId,
            }),
        );
        try {
            await s3.send(
                new GetBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Bucket: bucket,
                }),
            );
            throw new Error('Expected NoSuchTagSet error');
        } catch (err) {
            assertError(err, 'NoSuchTagSet');
        }
    });

    it('should put 50 tags', async () => {
        const tags = {
            TagSet: new Array(50).fill().map((el, index) => ({
                Key: `test_${index}`,
                Value: `value_${index}`,
            })),
        };
        await s3.send(
            new PutBucketTaggingCommand({
                AccountId: s3.AccountId,
                Tagging: tags,
                Bucket: bucket,
                ExpectedBucketOwner: s3.AccountId,
            }),
        );
    });

    it('should not put more than 50 tags', async () => {
        const tags = {
            TagSet: new Array(51).fill().map((el, index) => ({
                Key: `test_${index}`,
                Value: `value_${index}`,
            })),
        };
        try {
            await s3.send(
                new PutBucketTaggingCommand({
                    AccountId: s3.AccountId,
                    Tagging: tags,
                    Bucket: bucket,
                    ExpectedBucketOwner: s3.AccountId,
                }),
            );
            throw new Error('Expected BadRequest error');
        } catch (err) {
            assertError(err, 'BadRequest');
        }
    });
});
