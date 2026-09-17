'use strict';

const assert = require('assert');
const { createHash } = require('crypto');
const { v4: uuidv4 } = require('uuid');
const {
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    GetObjectCommand,
    ListObjectVersionsCommand,
} = require('@aws-sdk/client-s3');

const { versioning } = require('arsenal');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');

const { BackbeatRoutesClient, GetMetadataCommand, PutMetadataCommand } = require('@scality/cloudserverclient');

const { generateVersionId, encode: encodeVersionId } = versioning.VersionID;

const TEST_BUCKET = `bucket-cleanread-${uuidv4().split('-')[0]}`;
const LOCALIZED_BODY = 'localized';
const CANONICAL_ID = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
// the only location flagged "isCRR" in tests/locationConfig/locationConfigTests.json
const SOURCE_LOCATION = 'location-crr-v1';
const LOCAL_LOCATION = 'us-east-1';
const REPLICATED_BODY = 'waiting for its data to be copied over';

const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;

let backbeatClient;
const replicatedVersions = [];

function buildMetadataBody(versionId, dataStoreName) {
    return JSON.stringify({
        'content-length': Buffer.byteLength(REPLICATED_BODY),
        'content-type': 'text/plain',
        'last-modified': new Date().toISOString(),
        'content-md5': createHash('md5').update(REPLICATED_BODY).digest('hex'),
        'owner-id': CANONICAL_ID,
        'owner-display-name': 'test',
        versionId,
        dataStoreName,
        location: null,
        replicationInfo: {
            status: 'REPLICA',
            isReplica: true,
            backends: [],
            content: [],
            destination: '',
            storageClass: '',
            role: '',
            storageType: '',
            dataStoreVersionId: '',
        },
    });
}

// the mongo-processor replicating a version, then the data mover merging it once
// the data has been copied: same version id, the location rewritten to the local one
function writeVersion(key, versionId, dataStoreName) {
    if (dataStoreName === SOURCE_LOCATION) {
        replicatedVersions.push({ key, versionId });
    }
    return backbeatClient.send(
        new PutMetadataCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            VersionId: encodeVersionId(versionId),
            Body: buildMetadataBody(versionId, dataStoreName),
        }),
    );
}

async function currentVersion(key) {
    const res = await s3.send(new GetObjectCommand({ Bucket: TEST_BUCKET, Key: key }));
    return res.VersionId;
}

const describeIfCleanRead = process.env.S3METADATA === 'mongodb' ? describe : describe.skip;

describeIfCleanRead('clean read: localizing a replicated version', function testSuite() {
    this.timeout(120000);

    before(async () => {
        const creds = await s3.config.credentials();
        backbeatClient = new BackbeatRoutesClient({
            endpoint: `http://${process.env.IP || '127.0.0.1'}:8000`,
            region: 'us-east-1',
            credentials: {
                accessKeyId: creds.accessKeyId,
                secretAccessKey: creds.secretAccessKey,
            },
            forcePathStyle: true,
        });
        await s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }));
        await s3.send(
            new PutBucketVersioningCommand({
                Bucket: TEST_BUCKET,
                VersioningConfiguration: { Status: 'Enabled' },
            }),
        );
    });

    after(async () => {
        // the teardown lists through the S3 API, which hides the versions left
        // non-localized: localize them first, the way the data mover would
        await Promise.all(replicatedVersions.map(({ key, versionId }) => writeVersion(key, versionId, LOCAL_LOCATION)));
        await bucketUtil.empty(TEST_BUCKET);
        await bucketUtil.deleteOne(TEST_BUCKET);
    });

    it('should hide the replicated version, then serve it once localized', async () => {
        const key = 'clean-read-localization';
        const localized = await s3.send(new PutObjectCommand({ Bucket: TEST_BUCKET, Key: key, Body: LOCALIZED_BODY }));
        const replicatedVersionId = generateVersionId(`${process.pid}`, 'RG001');

        // the version is replicated, its data still on the source site
        await writeVersion(key, replicatedVersionId, SOURCE_LOCATION);

        // clean read hides it: the object still reads as the older localized version
        assert.strictEqual(await currentVersion(key), localized.VersionId);
        const versions = await s3.send(new ListObjectVersionsCommand({ Bucket: TEST_BUCKET, Prefix: key }));
        assert.deepStrictEqual(
            (versions.Versions || []).map(v => v.VersionId),
            [localized.VersionId],
        );

        // the data mover copies the data and merges the metadata: same version,
        // now pointing at the local location
        await writeVersion(key, replicatedVersionId, LOCAL_LOCATION);

        // it becomes the current version: the master was promoted
        assert.strictEqual(await currentVersion(key), encodeVersionId(replicatedVersionId));
    });

    it('should let the data mover read the replicated version by its id', async () => {
        const key = 'clean-read-by-version-id';
        await s3.send(new PutObjectCommand({ Bucket: TEST_BUCKET, Key: key, Body: LOCALIZED_BODY }));
        const replicatedVersionId = generateVersionId(`${process.pid}`, 'RG001');
        await writeVersion(key, replicatedVersionId, SOURCE_LOCATION);

        // hidden from the clients
        await assert.rejects(
            s3.send(
                new GetObjectCommand({
                    Bucket: TEST_BUCKET,
                    Key: key,
                    VersionId: encodeVersionId(replicatedVersionId),
                }),
            ),
            err => {
                assert.strictEqual(err.name, 'NoSuchVersion');
                return true;
            },
        );

        // but readable through the backbeat route, which is how it gets localized
        const res = await backbeatClient.send(
            new GetMetadataCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                VersionId: encodeVersionId(replicatedVersionId),
            }),
        );
        const md = JSON.parse(res.Body);
        assert.strictEqual(md.dataStoreName, SOURCE_LOCATION);
    });
});
