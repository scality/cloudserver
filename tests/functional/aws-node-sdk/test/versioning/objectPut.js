const assert = require('assert');
const { promisify } = require('util');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
    GetObjectTaggingCommand,
    DeleteObjectCommand,
    PutObjectAclCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');

const {
    createDualNullVersion,
    removeAllVersions,
    versioningEnabled,
    versioningSuspended,
    checkOneVersion,
} = require('../../lib/utility/versioning-util');
const customS3Request = require('../../lib/utility/customS3Request');

const data = ['foo1', 'foo2'];
const counter = 100;
const key = 'objectKey';

const removeAllVersionsAsync = promisify(removeAllVersions);
const createDualNullVersionAsync = promisify(createDualNullVersion);

describe('put and get object with versioning', function testSuite() {
    this.timeout(600000);

    withV4(sigCfg => {
        let s3;
        let bucket;

        beforeEach(async () => {
            s3 = new S3Client(getConfig('default', sigCfg));
            bucket = `versioning-bucket-${Date.now()}`;
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        });

        afterEach(async () => {
            await removeAllVersionsAsync({ Bucket: bucket });
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should return InvalidArgument for a request with versionId query', async () => {
            const params = { Bucket: bucket, Key: key, Body: '' };
            const query = { versionId: 'testVersionId' };
            try {
                await customS3Request(PutObjectCommand, params, { query });
                assert.fail('Expected error but did not find one');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.$metadata?.httpStatusCode, 400);
            }
        });

        it('should return InvalidArgument for a request with empty string versionId query', async () => {
            const params = { Bucket: bucket, Key: key, Body: '' };
            const query = { versionId: '' };
            try {
                await customS3Request(PutObjectCommand, params, { query });
                assert.fail('Expected error but did not find one');
            } catch (err) {
                assert.strictEqual(err.name, 'InvalidArgument');
                assert.strictEqual(err.$metadata?.httpStatusCode, 400);
            }
        });

        it('should put and get a non-versioned object without including version ids in response headers', async () => {
            const params = { Bucket: bucket, Key: key, Body: '' };
            const putRes = await s3.send(new PutObjectCommand({
                ...params,
                Body: '',
            }));
            assert.strictEqual(putRes.VersionId, undefined);

            const getRes = await s3.send(new GetObjectCommand(params));
            assert.strictEqual(getRes.VersionId, undefined);
        });

        it('version-specific get should still not return version id in response header', async () => {
            const params = { Bucket: bucket, Key: key, Body: '' };
            const putRes = await s3.send(new PutObjectCommand({
                ...params,
                Body: '',
            }));
            assert.strictEqual(putRes.VersionId, undefined);

            const getRes = await s3.send(new GetObjectCommand({
                Bucket: bucket,
                Key: key,
                VersionId: 'null',
            }));
            assert.strictEqual(getRes.VersionId, undefined);
        });

        describe('on a version-enabled bucket', () => {
            beforeEach(async () => {
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
            });

            it('should create a new version for an object', async () => {
                const params = { Bucket: bucket, Key: key, Body: '' };
                const putRes = await s3.send(new PutObjectCommand(params));

                const getRes = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: putRes.VersionId,
                }));

                assert.strictEqual(putRes.VersionId, getRes.VersionId,
                    'version ids are not equal');
            });

            it('should create a new version with tag set for an object', async () => {
                const tagKey = 'key1';
                const tagValue = 'value1';
                const putParams = {
                    Bucket: bucket,
                    Key: key,
                    Tagging: `${tagKey}=${tagValue}`,
                };

                const putRes = await s3.send(new PutObjectCommand(putParams));

                const tagRes = await s3.send(new GetObjectTaggingCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: putRes.VersionId,
                }));

                assert.strictEqual(tagRes.VersionId, putRes.VersionId,
                    'version ids are not equal');
                assert.strictEqual(tagRes.TagSet[0].Key, tagKey);
                assert.strictEqual(tagRes.TagSet[0].Value, tagValue);
            });
        });

        describe('on a version-enabled bucket with non-versioned object', () => {
            const eTags = [];

            beforeEach(async () => {
                const putRes = await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: data[0],
                }));
                eTags.length = 0;
                eTags.push(putRes.ETag);

                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
            });

            afterEach(() => {
                eTags.length = 0;
            });

            it('should get null (latest) version in versioning enabled ' +
            'bucket when version id is not specified', async () => {
                const res = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                }));

                assert.strictEqual(res.VersionId, 'null');
            });

           it('should get null version in versioning enabled bucket ' +
            'when version id is specified', async () => {
                const res = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));

                assert.strictEqual(res.VersionId, 'null');
            });

            it('should keep null version and create a new version', async () => {
                const putRes = await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: data[1],
                }));
                const newVersion = putRes.VersionId;
                eTags.push(putRes.ETag);

                const newVerRes = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: newVersion,
                }));
                assert.strictEqual(newVerRes.VersionId, newVersion,
                    'version ids are not equal');
                assert.strictEqual(newVerRes.ETag, eTags[1]);

                const nullRes = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));
                assert.strictEqual(nullRes.VersionId, 'null');
                assert.strictEqual(nullRes.ETag, eTags[0]);
            });

            it('should create new versions but still keep the null version', async () => {
                const params = { Bucket: bucket, Key: key, Body: '' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };

                for (let i = 0; i < counter; i++) {
                    const putRes = await s3.send(new PutObjectCommand(params));
                    assert(putRes.VersionId);

                    const nullVerData = await s3.send(new GetObjectCommand(paramsNull));
                    assert.strictEqual(nullVerData.ETag, eTags[0]);
                    assert.strictEqual(nullVerData.VersionId, 'null');
                }
            });

            // S3C-5139
            it('should not fail PUT on versioning-suspended bucket if nullVersionId refers ' +
            'to deleted null version', async () => {                
                // create a new version on top of non-versioned object
                await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                }));

                // suspend versioning
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));

                // delete existing non-versioned object
                await s3.send(new DeleteObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));

                // put a new null version
                const putRes = await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: data[0],
                }));
                eTags[0] = putRes.ETag;

                // get the new null version
                const nullVerData = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));
                assert.strictEqual(nullVerData.ETag, eTags[0]);
                assert.strictEqual(nullVerData.VersionId, 'null');
            });
        });

        describe('on version-suspended bucket', () => {
            beforeEach(async () => {
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));
            });

            it('should not return version id for new object', async () => {
                const params = { Bucket: bucket, Key: key, Body: 'foo' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };

                const putRes = await s3.send(new PutObjectCommand(params));
                const eTag = putRes.ETag;
                assert.strictEqual(putRes.VersionId, undefined);

                const nullVerData = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullVerData.ETag, eTag);
                assert.strictEqual(nullVerData.VersionId, 'null');
            });

            it('should update null version if put object twice', async () => {
                const params = { Bucket: bucket, Key: key, Body: '' };
                const params1 = { Bucket: bucket, Key: key, Body: data[0] };
                const params2 = { Bucket: bucket, Key: key, Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };
                const eTags = [];

                const putRes1 = await s3.send(new PutObjectCommand(params1));
                assert.strictEqual(putRes1.VersionId, undefined);
                eTags.push(putRes1.ETag);

                const masterRes = await s3.send(new GetObjectCommand(params));
                assert.strictEqual(masterRes.VersionId, 'null');
                assert.strictEqual(masterRes.ETag, eTags[0], 'wrong object data');

                const putRes2 = await s3.send(new PutObjectCommand(params2));
                assert.strictEqual(putRes2.VersionId, undefined);
                eTags.push(putRes2.ETag);

                const nullRes = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullRes.VersionId, 'null');
                assert.strictEqual(nullRes.ETag, eTags[1], 'wrong object data');

                const masterRes2 = await s3.send(new GetObjectCommand(params));
                assert.strictEqual(masterRes2.VersionId, 'null');
                assert.strictEqual(masterRes2.ETag, eTags[1], 'wrong object data');
            });

            // Jira issue: S3C-444
            it('put object after put object acl on null version which is ' +
            'latest version should not result in two null version with ' +
            'different version ids', async () => {                
                // create new null version (master version in metadata)
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: '' }));
                await checkOneVersion(s3, bucket, 'null');

                // apply ACL on null version
                await s3.send(new PutObjectAclCommand({
                    Bucket: bucket,
                    Key: key,
                    ACL: 'public-read-write',
                    VersionId: 'null',
                }));

                // before overwriting master version, put object should clean up latest null version
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: '' }));

                // if clean-up did not occur, would see two null versions with different version IDs
                await checkOneVersion(s3, bucket, 'null');
            });

            // Jira issue: S3C-444
            it('put object after creating dual null version another way ' +
            'should not result in two null version with different version ids', async () => {                
                // create dual null version state another way   
                await createDualNullVersionAsync(s3, bucket, key);

                // versioning is left enabled after above step
                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));

                // before overwriting master version, put object should clean up latest null version
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: '' }));

                // if clean-up did not occur, would see two null versions with different version IDs
                await checkOneVersion(s3, bucket, 'null');
            });
        });

        describe('on a version-suspended bucket with non-versioned object', () => {
            const eTags = [];

            beforeEach(async () => {
                const putRes = await s3.send(new PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: data[0],
                }));
                eTags.length = 0;
                eTags.push(putRes.ETag);

                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));
            });

            afterEach(() => {
                eTags.length = 0;
            });

            it('should get null version (latest) in versioning suspended bucket without specifying version id',
            async () => {
                const res = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                }));

                assert.strictEqual(res.VersionId, 'null');
            });

            it('should get null version in versioning suspended bucket specifying version id', async () => {
                const res = await s3.send(new GetObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                }));

                assert.strictEqual(res.VersionId, 'null');
            });

            it('should update null version in versioning suspended bucket', async () => {
                const params = { Bucket: bucket, Key: key, Body: '' };
                const putParams = { Bucket: bucket, Key: key, Body: data[1] };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };

                const nullRes1 = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullRes1.VersionId, 'null');

                const putRes = await s3.send(new PutObjectCommand(putParams));
                assert.strictEqual(putRes.VersionId, undefined);
                eTags.push(putRes.ETag);

                const nullRes2 = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullRes2.VersionId, 'null');
                assert.strictEqual(nullRes2.ETag, eTags[1], 'wrong object data');

                const masterRes = await s3.send(new GetObjectCommand(params));
                assert.strictEqual(masterRes.VersionId, 'null');
                assert.strictEqual(masterRes.ETag, eTags[1], 'wrong object data');
            });
        });

        describe('on versioning suspended then enabled bucket w/ null version', () => {
            const eTags = [];

            beforeEach(async () => {
                const params = { Bucket: bucket, Key: key, Body: data[0] };

                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningSuspended,
                }));

                const putRes = await s3.send(new PutObjectCommand(params));
                eTags.length = 0;
                eTags.push(putRes.ETag);

                await s3.send(new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: versioningEnabled,
                }));
            });

            afterEach(() => {
                eTags.length = 0;
            });

            it('should preserve the null version when creating new versions', async () => {
                const params = { Bucket: bucket, Key: key, Body: '' };
                const paramsNull = {
                    Bucket: bucket,
                    Key: key,
                    VersionId: 'null',
                };

                const nullVerData1 = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullVerData1.ETag, eTags[0]);
                assert.strictEqual(nullVerData1.VersionId, 'null');

                for (let i = 0; i < counter; i++) {
                    const putRes = await s3.send(new PutObjectCommand(params));
                    assert.notEqual(putRes.VersionId, undefined);
                }

                const nullVerData2 = await s3.send(new GetObjectCommand(paramsNull));
                assert.strictEqual(nullVerData2.ETag, eTags[0]);
            });

            it('should create a bunch of objects and their versions', async () => {
                const vids = [];
                const keycount = 50;
                const versioncount = 20;
                const value = '{"foo":"bar"}';

                for (let i = 0; i < keycount; i++) {
                    const keyName = `foo${i}`;
                    const params = { Bucket: bucket, Key: keyName, Body: value };

                    for (let j = 0; j < versioncount; j++) {
                        const putRes = await s3.send(new PutObjectCommand(params));
                        assert(putRes.VersionId, 'invalid versionId');
                        vids.push({ Key: keyName, VersionId: putRes.VersionId });
                    }
                }

                assert.strictEqual(vids.length, keycount * versioncount);
            });
        });
    });
});
