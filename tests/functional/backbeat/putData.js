'use strict';

const assert = require('assert');
const { createHash } = require('crypto');
const { v4: uuidv4 } = require('uuid');
const { CreateBucketCommand, PutBucketVersioningCommand, PutObjectCommand } = require('@aws-sdk/client-s3');

const { versioning } = require('arsenal');
const { ExternalNullVersionId } = versioning.VersioningConstants;
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');

const { BackbeatRoutesClient, PutDataCommand, VersionIdCollisionException } = require('@scality/cloudserverclient');

const { generateVersionId, encode: encodeVersionId } = versioning.VersionID;

const TEST_BUCKET = `bucket-putdata-${uuidv4().split('-')[0]}`;
const OBJECT_BODY = 'imAboutToBeCascadedWitNoParachuteInMyBack';
const OBJECT_MD5_HEX = createHash('md5').update(OBJECT_BODY).digest('hex');
const CANONICAL_ID = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;

let backbeatClient;

async function putData(key, { versionId } = {}) {
    return backbeatClient.send(
        new PutDataCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            ContentMD5: OBJECT_MD5_HEX,
            CanonicalID: CANONICAL_ID,
            VersioningRequired: true,
            VersionId: versionId || undefined,
            Body: Buffer.from(OBJECT_BODY),
        }),
    );
}

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

describe('putData : VersionId collision detection', () => {
    it('should throw VersionIdCollisionException with microVersionId when versionId matches master', async () => {
        const key = 'putdata-collision';
        const putResult = await s3.send(
            new PutObjectCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                Body: Buffer.from(OBJECT_BODY),
                ContentType: 'text/plain',
            }),
        );
        const encodedVersionId = putResult.VersionId;
        assert.ok(encodedVersionId, 'PutObject should return a VersionId');

        try {
            await putData(key, { versionId: encodedVersionId });
            assert.fail('expected VersionIdCollisionException');
        } catch (err) {
            assert.ok(
                err instanceof VersionIdCollisionException,
                `expected VersionIdCollisionException, got ${err.constructor.name}`,
            );
            // microVersionId in the error lets backbeat decide whether to proceed
            // with metadata-only replication or skip entirely (loop/stale detection).
            // '' signals the existing object has no microVersionId (original write state).
            assert.strictEqual(
                err.microVersionId,
                '',
                'microVersionId in exception should be empty when object has no microVersionId',
            );
        }
    });

    it('should write data normally when VersionId does not match the current master', async () => {
        const key = 'putdata-no-collision';

        await s3.send(
            new PutObjectCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                Body: Buffer.from(OBJECT_BODY),
                ContentType: 'text/plain',
            }),
        );

        const differentVersionId = encodeVersionId(generateVersionId('different-instance', 'RG001'));
        const output = await putData(key, { versionId: differentVersionId });
        assert.ok(output.Location, 'should return a Location when data is written normally');
    });
});

describe('putData : null-version objects (ExternalNullVersionId)', () => {
    // Null-version objects created before versioning was enabled use Arsenal constant ExternalNullVersionId = 'null'
    // getEncodedVersionId() returns 'null' as-is (no base62 encoding), and objMd.versionId is
    // undefined in metadata : collision detection is not possible, so putData must write normally.
    it('should write normally when VersionId is "null" (ExternalNullVersionId)', async () => {
        const output = await backbeatClient.send(
            new PutDataCommand({
                Bucket: TEST_BUCKET,
                Key: 'putdata-null-version',
                ContentMD5: OBJECT_MD5_HEX,
                CanonicalID: CANONICAL_ID,
                VersioningRequired: true,
                VersionId: ExternalNullVersionId,
                Body: Buffer.from(OBJECT_BODY),
            }),
        );
        assert.ok(output.Location, 'putData with null-version versionId should write normally');
    });
});

describe('putData : baseline (no cascade headers)', () => {
    it('should succeed normally when putData has no VersionId header', async () => {
        const key = 'putdata-baseline-no-version-id';
        const output = await putData(key);
        assert.ok(output.Location, 'putData without VersionId should return a Location');
    });

    it('should succeed when putData is sent without Expect: 100-continue (old-client compat)', async () => {
        const key = 'putdata-baseline-no-expect-continue';
        const output = await backbeatClient.send(
            new PutDataCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                ContentMD5: OBJECT_MD5_HEX,
                CanonicalID: CANONICAL_ID,
                VersioningRequired: true,
                Body: Buffer.from(OBJECT_BODY),
            }),
        );
        assert.ok(output.Location, 'putData without Expect: 100-continue should return a Location');
    });
});
