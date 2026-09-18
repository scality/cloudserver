const assert = require('assert');
const crypto = require('crypto');
const { versioning } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
const { config } = require('../../../../../lib/Config');
const metadata = require('../../../../../lib/metadata/wrapper');
const { initMetadata, getMetadata } = require('../utils/init');
const { DummyRequestLogger } = require('../../../../unit/helpers');
const { removeAllVersions } = require('../../lib/utility/versioning-util');
const { promisify } = require('util');

const versionIdUtils = versioning.VersionID;
const log = new DummyRequestLogger();
const removeAllVersionsAsync = promisify(removeAllVersions);

const bucket = `clean-read-bucket-${Date.now()}`;
const objectKey = 'clean-read-object';
const LOCALIZED_BODY = 'localized';
const NON_LOCALIZED_BODY = 'waiting for its data to be copied over';

// clean read is implemented by the mongodb metadata backend only
const describeIfCleanRead =
    process.env.S3METADATA === 'mongodb' && process.env.S3_CLEAN_READ_ENABLED === 'true' ? describe : describe.skip;

describeIfCleanRead('clean read', function testSuite() {
    this.timeout(600000);

    withV4(sigCfg => {
        let s3;
        let localizedVersionId;
        let nonLocalizedVersionId;

        // Replicates a version the way the clean-room mongo-processor does
        async function replicateNonLocalizedVersion() {
            const decodedVersionId = versionIdUtils.generateVersionId(`${process.pid}`, config.replicationGroupId);
            const objMD = await getMetadata(bucket, objectKey, localizedVersionId);
            objMD.versionId = decodedVersionId;
            // the version carries a content of its own, written on the source site
            objMD['content-length'] = NON_LOCALIZED_BODY.length;
            objMD['content-md5'] = crypto.createHash('md5').update(NON_LOCALIZED_BODY).digest('hex');
            // the only location flagged "isCRR" in tests/locationConfig/locationConfigTests.json
            objMD.dataStoreName = 'location-crr-v1';
            objMD['last-modified'] = new Date().toJSON();
            await new Promise((resolve, reject) =>
                metadata.putObjectMD(
                    bucket,
                    objectKey,
                    objMD,
                    { versionId: decodedVersionId, repairMaster: true },
                    log,
                    err => (err ? reject(err) : resolve()),
                ),
            );
            return versionIdUtils.encode(decodedVersionId);
        }

        before(async () => {
            s3 = new S3Client(getConfig('default', sigCfg));
            await initMetadata();
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            await s3.send(
                new PutBucketVersioningCommand({
                    Bucket: bucket,
                    VersioningConfiguration: { Status: 'Enabled' },
                }),
            );
            const localized = await s3.send(
                new PutObjectCommand({ Bucket: bucket, Key: objectKey, Body: LOCALIZED_BODY }),
            );
            localizedVersionId = localized.VersionId;
            nonLocalizedVersionId = await replicateNonLocalizedVersion();
        });

        after(async () => {
            await removeAllVersionsAsync({ Bucket: bucket });
            await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
        });

        it('should omit the non-localized version from the version listing', async () => {
            const res = await s3.send(new ListObjectVersionsCommand({ Bucket: bucket }));
            assert.deepStrictEqual(
                (res.Versions || []).map(version => version.VersionId),
                [localizedVersionId],
            );
        });

        it('should list the object, carried by its newest localized version', async () => {
            const res = await s3.send(new ListObjectsV2Command({ Bucket: bucket }));
            assert.strictEqual(res.Contents.length, 1);
            assert.strictEqual(res.Contents[0].Key, objectKey);
            // the size tells the two versions apart, the master having kept the
            // localized one rather than following the newer non-localized version
            assert.strictEqual(res.Contents[0].Size, LOCALIZED_BODY.length);
        });

        it('should serve the newest localized version as the current object', async () => {
            const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: objectKey }));
            assert.strictEqual(res.VersionId, localizedVersionId);
            assert.strictEqual(res.ContentLength, LOCALIZED_BODY.length);
            assert.strictEqual(await res.Body.transformToString(), LOCALIZED_BODY);
        });

        it('should reject a get on the non-localized version', async () => {
            await assert.rejects(
                s3.send(new GetObjectCommand({ Bucket: bucket, Key: objectKey, VersionId: nonLocalizedVersionId })),
                err => {
                    assert.strictEqual(err.name, 'NoSuchVersion');
                    return true;
                },
            );
        });

        it('should reject a head on the non-localized version', async () => {
            await assert.rejects(
                s3.send(new HeadObjectCommand({ Bucket: bucket, Key: objectKey, VersionId: nonLocalizedVersionId })),
                err => {
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                    return true;
                },
            );
        });
    });
});
