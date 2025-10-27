const assert = require('assert');
const {
    CreateBucketCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
    GetObjectTaggingCommand,
    PutBucketAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const checkError = require('../../lib/utility/checkError');
const { taggingTests } = require('../../lib/utility/tagging');

const bucketName = 'testputtaggingbucket';
const objectName = 'testputtaggingobject';
const objectNameAcl = 'testputtaggingobjectacl';

const taggingConfig = { TagSet: [
    {
        Key: 'key1',
        Value: 'value1',
    }] };

function generateMultipleTagConfig(number) {
    const tags = [];
    for (let i = 0; i < number; i++) {
        tags.push({ Key: `myKey${i}`, Value: `myValue${i}` });
    }
    return {
        TagSet: tags,
    };
}
function generateTaggingConfig(key, value) {
    return {
        TagSet: [
            {
                Key: key,
                Value: value,
            },
        ],
    };
}

describe('PUT object taggings', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        const otherAccountBucketUtility = new BucketUtility('lisa', {});
        const otherAccountS3 = otherAccountBucketUtility.s3;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
            await s3.send(new PutObjectCommand({ Bucket: bucketName, Key: objectName }));
        });

        afterEach(async () => {
            await bucketUtil.empty(bucketName);
            await bucketUtil.deleteOne(bucketName);
        });

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, async () => {
                const taggingConfig = generateTaggingConfig(
                    taggingTest.tag.key,
                    taggingTest.tag.value
                );
                
                if (taggingTest.error) {
                    try {
                        await s3.send(new PutObjectTaggingCommand({
                            Bucket: bucketName,
                            Key: objectName,
                            Tagging: taggingConfig
                        }));
                        assert.fail('Expected an error but request succeeded');
                    } catch (err) {
                        checkError(err, taggingTest.error, 400);
                    }
                } else {
                    const data = await s3.send(new PutObjectTaggingCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Tagging: taggingConfig
                    }));
                    assert.strictEqual(Object.keys(data).length, 1);
                }
            });
        });

        it('should allow putting 50 tags', async () => {
            const taggingConfig = generateMultipleTagConfig(50);
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig
            }));
        });

        it('should return BadRequest if putting more than 50 tags', async () => {
            const taggingConfig = generateMultipleTagConfig(51);
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfig
                }));
                assert.fail('Expected BadRequest error');
            } catch (err) {
                checkError(err, 'BadRequest', 400);
            }
        });


        it('should put tag set', async () => {
            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: taggingConfig,
            }));

            const data = await s3.send(new GetObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
            }));

            assert.deepStrictEqual(data.TagSet, taggingConfig.TagSet);
        });

        it('should return InvalidTag if using the same key twice', async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging:  { TagSet: [
                        {
                            Key: 'key1',
                            Value: 'value1',
                        },
                        {
                            Key: 'key1',
                            Value: 'value2',
                        },
                    ] },
                }));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                checkError(err, 'InvalidTag', 400);
            }
        });

        it('should return InvalidTag if key is an empty string', async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: {
                        TagSet: [
                            {
                                Key: '',
                                Value: 'value1',
                            },
                        ]
                    }
                }));
                assert.fail('Expected InvalidTag error');
            } catch (err) {
                checkError(err, 'InvalidTag', 400);
            }
        });

        it('should be able to put an empty Tag set', async () => {
            const data = await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectName,
                Tagging: { TagSet: [] }
            }));
            assert.strictEqual(data.$metadata.httpStatusCode, 200);
        });

        it('should return NoSuchKey put tag to a non-existing object',
        async () => {
            try {
                await s3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: 'nonexisting',
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected NoSuchKey error');
            } catch (err) {
                checkError(err, 'NoSuchKey', 404);
            }
        });

        it('should return 403 AccessDenied putting tag with another account',
        async () => {
            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied putting tag with a different ' +
        'account to an object with ACL "public-read-write"',
        async () => {
            await s3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectName,
                ACL: 'public-read-write',
            }));

            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should return 403 AccessDenied putting tag to an object ' +
        ' in a bucket created with a different account',
        async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            try {
                await otherAccountS3.send(new PutObjectTaggingCommand({
                    Bucket: bucketName,
                    Key: objectNameAcl,
                    Tagging: taggingConfig,
                }));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                checkError(err, 'AccessDenied', 403);
            }
        });

        it('should put tag to an object in a bucket created with same ' +
        'account', async () => {
            await s3.send(new PutBucketAclCommand({
                Bucket: bucketName,
                ACL: 'public-read-write',
            }));
            await otherAccountS3.send(new PutObjectCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
            }));

            await s3.send(new PutObjectTaggingCommand({
                Bucket: bucketName,
                Key: objectNameAcl,
                Tagging: taggingConfig,
            }));
        });
    });
});
