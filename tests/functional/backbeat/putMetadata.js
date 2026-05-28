'use strict';

const assert = require('assert');
const { createHash } = require('crypto');
const { v4: uuidv4 } = require('uuid');
const {
    CreateBucketCommand,
    PutBucketReplicationCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    PutObjectTaggingCommand,
} = require('@aws-sdk/client-s3');

const { versioning, models } = require('arsenal');
const { ObjectMD } = models;
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');

const {
    BackbeatRoutesClient,
    GetMetadataCommand,
    PutMetadataCommand,
    MicroVersionIdAlreadyStoredException,
    StaleMicroVersionIdException,
} = require('@scality/cloudserverclient');

const { generateVersionId, encode: encodeVersionId } = versioning.VersionID;

const TEST_BUCKET = `bucket-putmetadata-${uuidv4().split('-')[0]}`;
const TEST_BUCKET_CRR = `bucket-putmetadata-crr-${uuidv4().split('-')[0]}`;
const DEST_BUCKET = `bucket-putmetadata-dest-${uuidv4().split('-')[0]}`;
const OBJECT_BODY = 'imAboutToBeCascadedWitNoParachuteInMyBack';
const OBJECT_MD5_HEX = createHash('md5').update(OBJECT_BODY).digest('hex');
const CANONICAL_ID = '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const bucketUtil = new BucketUtility('default', {});
const s3 = bucketUtil.s3;

let backbeatClient;

function makeMicroVersionId() {
    const raw = generateVersionId('test-instance', 'RG001');
    return { raw, encoded: encodeVersionId(raw) };
}

function buildMetadataBody(overrides) {
    const obj = Object.assign({
        'content-length': Buffer.byteLength(OBJECT_BODY),
        'content-type': 'text/plain',
        'last-modified': new Date().toISOString(),
        'x-amz-version-id': 'null',
        'owner-id': CANONICAL_ID,
        'owner-display-name': 'test',
        'content-md5': OBJECT_MD5_HEX,
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
    }, overrides || {});
    return Buffer.from(JSON.stringify(obj));
}

// putMetadata without x-scal-replication-content — tests pure conditional update semantics
async function putMetadata(key, mvId) {
    const bodyOverrides = mvId ? { microVersionId: mvId.raw } : {};
    return backbeatClient.send(new PutMetadataCommand({
        Bucket: TEST_BUCKET,
        Key: key,
        MicroVersionId: mvId ? mvId.encoded : undefined,
        Body: buildMetadataBody(bodyOverrides),
    }));
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
    await s3.send(new PutBucketVersioningCommand({
        Bucket: TEST_BUCKET,
        VersioningConfiguration: { Status: 'Enabled' },
    }));

    await s3.send(new CreateBucketCommand({ Bucket: DEST_BUCKET }));
    await s3.send(new PutBucketVersioningCommand({
        Bucket: DEST_BUCKET,
        VersioningConfiguration: { Status: 'Enabled' },
    }));

    await s3.send(new CreateBucketCommand({ Bucket: TEST_BUCKET_CRR }));
    await s3.send(new PutBucketVersioningCommand({
        Bucket: TEST_BUCKET_CRR,
        VersioningConfiguration: { Status: 'Enabled' },
    }));
    await s3.send(new PutBucketReplicationCommand({
        Bucket: TEST_BUCKET_CRR,
        ReplicationConfiguration: {
            Role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
            Rules: [{
                Status: 'Enabled',
                Prefix: '',
                Destination: {
                    Bucket: `arn:aws:s3:::${DEST_BUCKET}`,
                    StorageClass: 'zenko',
                },
            }],
        },
    }));
});

// These tests send no x-scal-replication-content header — microVersionId is used
// purely as a conditional update mechanism, independent of replication.
describe('putMetadata : microVersionId conditional updates (no replication content)', () => {
    it('should succeed on first write when destination has no microVersionId', async () => {
        const key = 'putmetadata-cond-first-write';
        const mvId = makeMicroVersionId();
        await putMetadata(key, mvId);
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        assert.strictEqual(new ObjectMD(JSON.parse(Body)).getMicroVersionId(), mvId.raw);
    });

    it('should succeed on first write when source has no microVersionId (putObject replication)', async () => {
        const key = 'putmetadata-cond-first-write-null-mvid';
        // '' = source has no microVersionId (putObject state): must not be treated as a loop
        await backbeatClient.send(new PutMetadataCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            MicroVersionId: '',
            Body: buildMetadataBody({}),
        }));
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        assert.strictEqual(new ObjectMD(JSON.parse(Body)).getMicroVersionId(), undefined,
            'stored object should have no microVersionId when source had none');
    });

    it('should throw MicroVersionIdAlreadyStoredException on second write with the same microVersionId',
        async () => {
        const key = 'putmetadata-cond-loop';
        const mvId = makeMicroVersionId();
        await putMetadata(key, mvId);
        await assert.rejects(
            () => putMetadata(key, mvId),
            err => {
                assert.ok(err instanceof MicroVersionIdAlreadyStoredException,
                    'second write with same id should throw MicroVersionIdAlreadyStoredException');
                return true;
            },
        );
    });

    it('should throw StaleMicroVersionIdException when writing with an older microVersionId', async () => {
        const key = 'putmetadata-cond-stale';
        const olderMvId = makeMicroVersionId();
        const newerMvId = makeMicroVersionId();
        await putMetadata(key, newerMvId);
        await assert.rejects(
            () => putMetadata(key, olderMvId),
            err => {
                assert.ok(err instanceof StaleMicroVersionIdException,
                    `expected StaleMicroVersionIdException, got ${err.constructor.name}`);
                return true;
            },
        );
    });

    it('should succeed and overwrite when writing with a newer microVersionId', async () => {
        const key = 'putmetadata-cond-forward';
        const olderMvId = makeMicroVersionId();
        const newerMvId = makeMicroVersionId();
        await putMetadata(key, olderMvId);
        await putMetadata(key, newerMvId);
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        assert.strictEqual(new ObjectMD(JSON.parse(Body)).getMicroVersionId(), newerMvId.raw,
            'stored microVersionId should be the newer one');
    });

    it('should return 400 when MicroVersionId header does not match body microVersionId', async () => {
        const key = 'putmetadata-cond-header-body-mismatch';
        // The mismatch check only fires when the object already exists (objMd non-null),
        // because incomingMicroVersionId is only decoded in that path.
        const initialMvId = makeMicroVersionId();
        await putMetadata(key, initialMvId);
        const headerMvId = makeMicroVersionId();
        const bodyMvId = makeMicroVersionId();
        await assert.rejects(
            () => backbeatClient.send(new PutMetadataCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                MicroVersionId: headerMvId.encoded,
                Body: buildMetadataBody({ microVersionId: bodyMvId.raw }),
            })),
            err => {
                assert.strictEqual(err.$metadata.httpStatusCode, 400,
                    'mismatched header and body microVersionId should return 400');
                return true;
            },
        );
    });

    it('should throw StaleMicroVersionIdException after objectPutTagging bumped microVersionId',
        async () => {
        const key = 'putmetadata-cond-stale-tagging';

        await s3.send(new PutObjectCommand({
            Bucket: TEST_BUCKET_CRR,
            Key: key,
            Body: Buffer.from(OBJECT_BODY),
            ContentType: 'text/plain',
        }));

        const olderMvId = makeMicroVersionId();

        await s3.send(new PutObjectTaggingCommand({
            Bucket: TEST_BUCKET_CRR,
            Key: key,
            Tagging: { TagSet: [{ Key: 'crr', Value: 'cascade' }] },
        }));

        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET_CRR, Key: key }));
        assert.ok(new ObjectMD(JSON.parse(Body)).getMicroVersionId(),
            'objectPutTagging should have set a microVersionId');

        await assert.rejects(
            () => backbeatClient.send(new PutMetadataCommand({
                Bucket: TEST_BUCKET_CRR,
                Key: key,
                MicroVersionId: olderMvId.encoded,
                Body: buildMetadataBody({ microVersionId: olderMvId.raw }),
            })),
            err => {
                assert.ok(err instanceof StaleMicroVersionIdException,
                    `expected StaleMicroVersionIdException, got ${err.constructor.name}`);
                return true;
            },
        );
    });
});

// These tests send x-scal-replication-content to simulate backbeat replication writes.
// microVersionId conditional update semantics apply on top of the replication behavior.
describe('putMetadata : cascade replication behavior (with replication content)', () => {
    it('should succeed on first write of a zero-byte object without replication content', async () => {
        const key = 'putmetadata-crr-zero-byte';
        await backbeatClient.send(new PutMetadataCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            MicroVersionId: '',
            Body: buildMetadataBody({ 'content-length': 0, location: null }),
        }));
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        assert.strictEqual(new ObjectMD(JSON.parse(Body)).getContentLength(), 0,
            'zero-byte object should be stored correctly');
    });

    it('should throw MicroVersionIdAlreadyStoredException on loop even with replication content',
        async () => {
        // Verifies that microVersionId loop detection fires regardless of whether
        // x-scal-replication-content is set
        const key = 'putmetadata-crr-loop';
        const mvId = makeMicroVersionId();
        await putMetadata(key, mvId);
        await assert.rejects(
            () => backbeatClient.send(new PutMetadataCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                MicroVersionId: mvId.encoded,
                ReplicationContent: 'METADATA',
                Body: buildMetadataBody({ microVersionId: mvId.raw }),
            })),
            err => {
                assert.ok(err instanceof MicroVersionIdAlreadyStoredException,
                    'loop detection should fire even with x-scal-replication-content set');
                return true;
            },
        );
    });

    it('should throw StaleMicroVersionIdException on stale even with replication content', async () => {
        const key = 'putmetadata-crr-stale';
        const olderMvId = makeMicroVersionId();
        const newerMvId = makeMicroVersionId();
        await putMetadata(key, newerMvId);
        await assert.rejects(
            () => backbeatClient.send(new PutMetadataCommand({
                Bucket: TEST_BUCKET,
                Key: key,
                MicroVersionId: olderMvId.encoded,
                ReplicationContent: 'METADATA',
                Body: buildMetadataBody({ microVersionId: olderMvId.raw }),
            })),
            err => {
                assert.ok(err instanceof StaleMicroVersionIdException,
                    'stale detection should fire even with x-scal-replication-content set');
                return true;
            },
        );
    });

    it('should clear replicationInfo when no CRR rules match on destination bucket', async () => {
        const key = 'putmetadata-crr-no-rules';
        const olderMvId = makeMicroVersionId();
        const newerMvId = makeMicroVersionId();
        // First write creates the object (required before METADATA-content writes).
        await putMetadata(key, olderMvId);
        // Second write with x-scal-replication-content triggers the cascade block.
        await backbeatClient.send(new PutMetadataCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            MicroVersionId: newerMvId.encoded,
            ReplicationContent: 'METADATA',
            Body: buildMetadataBody({ microVersionId: newerMvId.raw }),
        }));
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        const storedMd = new ObjectMD(JSON.parse(Body));
        assert.strictEqual(storedMd.getReplicationStatus(), '',
            'replication status should be cleared when no CRR rules match');
        assert.deepStrictEqual(storedMd.getReplicationBackends(), [],
            'replication backends should be empty when no CRR rules match');
        assert.strictEqual(storedMd.getReplicationIsReplica(), true,
            'isReplica should be preserved regardless of cascade triggering');
    });

    it('should set replication status to PENDING and preserve isReplica when bucket has CRR rules',
        async () => {
        const key = 'putmetadata-crr-next-hop';
        const olderMvId = makeMicroVersionId();
        const newerMvId = makeMicroVersionId();

        await backbeatClient.send(new PutMetadataCommand({
            Bucket: TEST_BUCKET_CRR,
            Key: key,
            MicroVersionId: olderMvId.encoded,
            Body: buildMetadataBody({ microVersionId: olderMvId.raw }),
        }));
        await backbeatClient.send(new PutMetadataCommand({
            Bucket: TEST_BUCKET_CRR,
            Key: key,
            MicroVersionId: newerMvId.encoded,
            ReplicationContent: 'METADATA',
            Body: buildMetadataBody({ microVersionId: newerMvId.raw }),
        }));

        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET_CRR, Key: key }));
        const storedMd = new ObjectMD(JSON.parse(Body));
        assert.strictEqual(storedMd.getMicroVersionId(), newerMvId.raw,
            'stored microVersionId should be the newer one');
        assert.strictEqual(storedMd.getReplicationStatus(), 'PENDING',
            'replication status should be PENDING when a CRR rule matches');
        assert.ok(storedMd.getReplicationBackends().length > 0,
            'replication backends should be populated when a CRR rule matches');
        assert.strictEqual(storedMd.getReplicationIsReplica(), true,
            'isReplica should be preserved regardless of cascade triggering');
    });
});

describe('putMetadata : baseline (no cascade headers)', () => {
    it('should succeed normally when putMetadata has no MicroVersionId header', async () => {
        const key = 'putmetadata-baseline-no-mvid';
        await putMetadata(key, null);
    });

    it('should not set a microVersionId on a regular S3 PutObject', async () => {
        const key = 'putmetadata-baseline-s3put';
        await s3.send(new PutObjectCommand({
            Bucket: TEST_BUCKET,
            Key: key,
            Body: Buffer.from(OBJECT_BODY),
        }));
        const { Body } = await backbeatClient.send(
            new GetMetadataCommand({ Bucket: TEST_BUCKET, Key: key }));
        assert.strictEqual(new ObjectMD(JSON.parse(Body)).getMicroVersionId(), undefined,
            'a regular S3 PutObject should not set a microVersionId');
    });
});
