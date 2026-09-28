const assert = require('assert');
const {
    PutObjectCommand,
    PutBucketVersioningCommand,
    HeadObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { fakeMetadataArchive, fakeMetadataTransition, getMetadata, initMetadata } = require('../utils/init');

const coldStateScenarios = [
    {
        name: 'transition in progress',
        transitionInProgress: true,
    },
    {
        name: 'archived',
        archiveState: {
            archiveInfo: { archiveId: 'archive-1' },
            restoreRequestedAt: new Date(0).toISOString(),
            restoreRequestedDays: 5,
        },
    },
    {
        name: 'restored (not expired)',
        archiveState: {
            archiveInfo: { archiveId: 'archive-restored' },
            restoreRequestedAt: new Date(0).toISOString(),
            restoreRequestedDays: 5,
            restoreCompletedAt: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
            restoreWillExpireAt: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString(),
        },
    },
    {
        name: 'restored (expired)',
        archiveState: {
            archiveInfo: { archiveId: 'archive-expired' },
            restoreRequestedAt: new Date(0).toISOString(),
            restoreRequestedDays: 5,
            restoreCompletedAt: new Date(Date.now() - 48 * 60 * 60 * 1000).toISOString(),
            restoreWillExpireAt: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString(),
        },
    },
];

const objectName = 'someObject';
const firstPutMetadata = {
    firstput: 'firstValue',
    firstputagain: 'firstValue',
    evenmoreonfirst: 'stuff',
};
const secondPutMetadata = {
    secondput: 'secondValue',
    secondputagain: 'secondValue',
};

describe('Put object with same key as prior object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let bucketName;

        before(async () => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            bucketName = await bucketUtil.createRandom(1);
            await initMetadata();
        });

        beforeEach(async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'I am the best content ever',
                    Metadata: firstPutMetadata,
                }),
            );
            const res = await s3.send(
                new HeadObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }),
            );
            assert.deepStrictEqual(res.Metadata, firstPutMetadata);
        });

        afterEach(async () => await bucketUtil.empty(bucketName));

        after(async () => await bucketUtil.deleteOne(bucketName));

        it('should overwrite all user metadata and data on overwrite put', async () => {
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'Much different',
                    Metadata: secondPutMetadata,
                }),
            );
            const res = await s3.send(
                new GetObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }),
            );
            assert.deepStrictEqual(res.Metadata, secondPutMetadata);
            const bodyText = await res.Body.transformToString();
            assert.deepStrictEqual(bodyText, 'Much different');
        });

        coldStateScenarios.forEach(({ name, transitionInProgress, archiveState }) => {
            it(`should replace object with cold-state metadata (${name}) in non-versioned bucket`, async () => {
                if (transitionInProgress) {
                    await fakeMetadataTransition(bucketName, objectName, undefined);
                } else {
                    await fakeMetadataArchive(bucketName, objectName, undefined, archiveState);
                }

                await s3.send(
                    new PutObjectCommand({
                        Bucket: bucketName,
                        Key: objectName,
                        Body: `overwrite cold state ${name}`,
                        Metadata: secondPutMetadata,
                    }),
                );

                const currentMD = await getMetadata(bucketName, objectName, undefined);
                assert.strictEqual(currentMD.archive, undefined);
                assert.strictEqual(currentMD['x-amz-scal-transition-in-progress'], undefined);
            });
        });

        it('should create a new version when replacing archived current object in versioned bucket', async () => {
            await bucketUtil.empty(bucketName);

            await s3.send(
                new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }),
            );

            const firstPutRes = await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'versioned first payload',
                    Metadata: firstPutMetadata,
                }),
            );
            assert(firstPutRes.VersionId);

            await fakeMetadataArchive(bucketName, objectName, undefined, {
                archiveInfo: { archiveId: 'archive-versioned-current' },
                restoreRequestedAt: new Date(0).toISOString(),
                restoreRequestedDays: 5,
            });

            const secondPutRes = await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'versioned second payload',
                    Metadata: secondPutMetadata,
                }),
            );
            assert(secondPutRes.VersionId);
            assert.notStrictEqual(secondPutRes.VersionId, firstPutRes.VersionId);

            const headRes = await s3.send(
                new HeadObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                }),
            );
            assert.deepStrictEqual(headRes.Metadata, secondPutMetadata);

            const currentMD = await getMetadata(bucketName, objectName, undefined);
            assert.strictEqual(currentMD.archive, undefined);
        });

        it('should replace archived current null version in version-suspended bucket', async () => {
            await bucketUtil.empty(bucketName);

            await s3.send(
                new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }),
            );
            await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'enabled-version-payload',
                }),
            );
            await s3.send(
                new PutBucketVersioningCommand({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Suspended' },
                }),
            );

            await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'null-current-before-archive',
                }),
            );

            await fakeMetadataArchive(bucketName, objectName, undefined, {
                archiveInfo: { archiveId: 'archive-null-current' },
                restoreRequestedAt: new Date(0).toISOString(),
                restoreRequestedDays: 5,
            });

            await s3.send(
                new PutObjectCommand({
                    Bucket: bucketName,
                    Key: objectName,
                    Body: 'replace archived null current',
                    Metadata: secondPutMetadata,
                }),
            );

            const currentMD = await getMetadata(bucketName, objectName, undefined);
            assert.strictEqual(currentMD.archive, undefined);
            assert.deepStrictEqual(currentMD['x-amz-meta-secondput'], secondPutMetadata.secondput);
        });
    });
});
