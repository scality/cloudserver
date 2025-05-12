/* eslint-disable */
const getConfig = require('../aws-node-sdk/test/support/config');
const { S3 } = require('aws-sdk');
const kms = require('../../../lib/kms/wrapper');
const { promisify } = require('util');
const BucketInfo = require('arsenal').models.BucketInfo;
const { DummyRequestLogger } = require('../../unit/helpers');
const BucketUtility = require('../aws-node-sdk/lib/utility/bucket-util');
const assert = require('assert');
const metadata = require('../../../lib/metadata/wrapper');
const crypto = require('crypto');
const log = new DummyRequestLogger();
const { config } = require('../../../lib/Config');
const { getKeyIdFromArn } = require('arsenal/build/lib/network/KMSInterface');

// use file to defined key in arn prefix, if no prefix mem is used

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests
// around SSE Key prefix and migration
// always getObject to ensure decryption

function getKey(key) {
    return config.kmsHideScalityArn ? getKeyIdFromArn(key) : key;
}

const testCases = [
    {
        name: 'algo-none',
        // as the init insert objects with each encryption
        // this bucket will have a non mandatory AES256
    },
    {
        name: 'algo-none-del-sse',
        /** flag to remove non mandatory AES256 SS3 from bucket MD beforeEach test */
        deleteSSE: true,
    },
    {
        name: 'algo-aes256',
        algo: 'AES256',
    },
    {
        name: 'algo-awskms',
        algo: 'aws:kms',
    },
    {
        name: 'algo-awskms-key',
        algo: 'aws:kms',
        masterKeyId: true,
    },
    {
        name: 'algo-awskms-key-arnprefix',
        algo: 'aws:kms',
        masterKeyId: true,
        arnPrefix: true,
    },
];
const testCasesObj = testCases.filter(tc => !tc.deleteSSE);

const s3config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(s3config);
const bucketUtil = new BucketUtility();

kms.client._supportsDefaultKeyPerAccount = false; // To generate keys without vault account side effect

// Fix for before migration run
// if (!kms.arnPrefix) kms.arnPrefix = '';

const memKms = require('../../../lib/kms/in_memory/backend').backend;


function hydrateSSEConfig({ algo: SSEAlgorithm, masterKeyId: KMSMasterKeyID }) {
    // stringify and parse to strip undefined values
    return JSON.parse(JSON.stringify({ Rules: [{
        ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm,
            KMSMasterKeyID,
        },
    }] }));
}

function putObjParams(Bucket, Key, sseConfig, kmsKeyId) {
    return {
        Bucket,
        Key,
        ...(sseConfig.algo && {
            ServerSideEncryption: sseConfig.algo,
            ...(sseConfig.masterKeyId && {
                SSEKMSKeyId: kmsKeyId,
            }),
        }),
    };
}

const getBucketMD = promisify(metadata.getBucket.bind(metadata));
const getObjectMD = promisify(metadata.getObjectMD.bind(metadata));
const updateBucketMD = promisify(metadata.updateBucket.bind(metadata));

async function getBucketSSE(Bucket) {
    const sse = await s3.getBucketEncryption({ Bucket }).promise();
    return sse
        .ServerSideEncryptionConfiguration
        .Rules[0]
        .ApplyServerSideEncryptionByDefault;
}

async function getObjectMDSSE(Bucket, Key) {
    // todo version ?
    const objMD = await getObjectMD(Bucket, Key, {}, log);

    const sse = objMD['x-amz-server-side-encryption'];
    const key = objMD['x-amz-server-side-encryption-aws-kms-key-id'];

    return {
        ServerSideEncryption: sse,
        SSEKMSKeyId: key,
    };
}

async function putEncryptedObject(Bucket, Key, sseConfig, kmsKeyId, Body) {
    return s3.putObject({
        ...putObjParams(Bucket, Key, sseConfig, kmsKeyId),
        Body,
    }).promise();
}

async function assertObjectSSEMigration(Bucket, Key, objConf, obj, bktConf, bkt, VersionId, Body) {
    const sseMD = await getObjectMDSSE(Bucket, Key);
    const head = await s3.headObject({ Bucket, Key, VersionId }).promise();
    const sseMDMigrated = await getObjectMDSSE(Bucket, Key);
    const expectedKey = `${sseMD.SSEKMSKeyId && sseMD.SSEKMSKeyId.startsWith('arn:scality:kms')
        ? '' : kms.arnPrefix}${sseMD.SSEKMSKeyId}`;

    if (sseMD.SSEKMSKeyId) {
        // assert.doesNotMatch(sseMD.SSEKMSKeyId, /^arn:scality:kms/);
    }

    // obj precedence over bkt
    assert.strictEqual(head.ServerSideEncryption, (objConf.algo || bktConf.algo));

    if (sseMDMigrated.SSEKMSKeyId) {
        assert.strictEqual(sseMDMigrated.SSEKMSKeyId, expectedKey);
    }

    if (obj.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, getKey(expectedKey));
    } else if (objConf.algo !== 'AES256' && bkt.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, getKey(expectedKey));
    } else if (head.ServerSideEncryption === 'aws:kms') {
        // We differ from aws behavior and always return a
        // masterKeyId even when not explicitly configured.
        assert.strictEqual(head.SSEKMSKeyId, getKey(expectedKey));
    } else {
        assert.strictEqual(head.SSEKMSKeyId, undefined);
    }

    // always verify GetObject as well to ensure acurate decryption
    const get = await s3.getObject({ Bucket, Key, ...(VersionId && { VersionId }) }).promise();
    assert.strictEqual(get.Body.toString(), Body);
}

/** for kms createBucketKey */
const bucketInfo = new BucketInfo('enc-bucket-test', 'OwnerId',
    'OwnerDisplayName', new Date().toJSON());

describe('SSE KMS migration', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const copyObj = 'copy-obj';
    let copyKmsKey;
    const bkts = {};
    const mpuCopyBkt = 'enc-bkt-mpu-copy';

    this.checkInitBucket = async function checkInitBucket(bktConf) {
        const bkt = {
            name: `enc-bkt-${bktConf.name}`,
            /** versioned bucket name */
            vname: `versioned-enc-bkt-${bktConf.name}`,
            kmsKeyInfo: null,
            kmsKey: null,
            /** For copy has source, include an object with each encryption */
            objs: {},
        };
        bkts[bktConf.name] = bkt;
        if (bktConf.algo && bktConf.masterKeyId) {
            bkt.kmsKeyInfo = await promisify(kms.createBucketKey)(bucketInfo, log);
            bkt.kmsKey = bktConf.arnPrefix
                ? bkt.kmsKeyInfo.masterKeyArn
                : bkt.kmsKeyInfo.masterKeyId;
        }
        void await s3.headBucket(({ Bucket: bkt.name })).promise();
        void await s3.headBucket(({ Bucket: bkt.vname })).promise();
        if (bktConf.algo) {
            // bucket encryption will be asserted in bucket test
            const bktSSE = await getBucketSSE(bkt.name);
            assert.strictEqual(bktSSE.SSEAlgorithm, bktConf.algo);
            if (bktSSE.KMSMasterKeyID) {
                // assert.doesNotMatch(bktSSE.KMSMasterKeyID, /^arn:scality:kms/);
            }

            const vbktSSE = await getBucketSSE(bkt.vname);
            assert.strictEqual(vbktSSE.SSEAlgorithm, bktConf.algo);
            if (vbktSSE.KMSMasterKeyID) {
                // assert.doesNotMatch(vbktSSE.KMSMasterKeyID, /^arn:scality:kms/);
            }
        }

        // Put an object for each SSE conf in each bucket
        void await Promise.all(testCases.map(async objConf => {
            const obj = {
                name: `for-copy-enc-obj-${objConf.name}`,
                kmsKeyInfo: null,
                kmsKey: null,
                body: `BODY(for-copy-enc-obj-${objConf.name})`,
            };
            bkt.objs[objConf.name] = obj;
            if (objConf.algo && objConf.masterKeyId) {
                obj.kmsKeyInfo = await promisify(kms.createBucketKey)(bucketInfo, log);
                obj.kmsKey = objConf.arnPrefix
                    ? obj.kmsKeyInfo.masterKeyArn
                    : obj.kmsKeyInfo.masterKeyId;
            }
            const objSSE = await getObjectMDSSE(bkt.name, obj.name);
            assert.strictEqual(objSSE.ServerSideEncryption, objConf.algo || bktConf.algo || '');
            // assert.doesNotMatch(objSSE.SSEKMSKeyId, /^arn:scality:kms/);
            return undefined;
        }));
    };

    before(async () => {
        copyKmsKey = (await promisify(kms.createBucketKey)(bucketInfo, log)).masterKeyArn;
        void await promisify(metadata.setup.bind(metadata))();

        void await s3.headBucket({ Bucket: copyBkt }).promise();
        void await s3.headBucket(({ Bucket: mpuCopyBkt })).promise();
        const copySSE = await s3.getBucketEncryption({ Bucket: copyBkt }).promise();
        const { SSEAlgorithm, KMSMasterKeyID } = copySSE
            .ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault;
        assert.strictEqual(SSEAlgorithm, 'aws:kms');
        // assert.doesNotMatch(KMSMasterKeyID, /^arn:scality:kms/);

        // Check Prepare every buckets with 1 object (for copy)
        void await Promise.all(testCases.map(async bktConf => this.checkInitBucket(bktConf)));
    });

    testCases.forEach(bktConf => describe(`bucket enc-bkt-${bktConf.name}`, () => {
        let bkt = bkts[bktConf.name];

        before(() => {
            bkt = bkts[bktConf.name];
        });

        if (bktConf.deleteSSE) {
            beforeEach(async () => {
                const bucketMD = await getBucketMD(bkt.name, log);
                if (bucketMD.getServerSideEncryption()) {
                    bucketMD.setServerSideEncryption(null);
                    void await updateBucketMD(bucketMD.getName(), bucketMD, log);
                }
            });
        }

        if (!bktConf.algo) {
            it('GetBucketEncryption should return ServerSideEncryptionConfigurationNotFoundError', async () => {
                void await assert.rejects(s3.getBucketEncryption({ Bucket: bkt.name }).promise(), err => {
                    assert.strictEqual(err.code, 'ServerSideEncryptionConfigurationNotFoundError');
                    return true;
                });
            });

            if (!bktConf.deleteSSE) {
                it('should have non mandatory SSE in bucket MD as test init put an object with AES256', async () => {
                    const bucketMD = await getBucketMD(bkt.name, log);
                    const sseMD = bucketMD.getServerSideEncryption();
                    assert.strictEqual(sseMD.mandatory, false);
                    assert.strictEqual(sseMD.algorithm, 'AES256');
                    assert.doesNotMatch(sseMD.masterKeyId, /^arn:scality:kms/);
                });
            }
        } else {
            it('ensure old SSE KMS key setup', async () => {
                const bucketMD = await getBucketMD(bkt.name, log);
                const sseMD = bucketMD.getServerSideEncryption();
                const sseS3 = await getBucketSSE(bkt.name);

                assert.strictEqual(sseS3.SSEAlgorithm, bktConf.algo);
                assert.strictEqual(sseMD.algorithm, bktConf.algo);
                if (!bktConf.masterKeyId) {
                    // AES256 or aws:kms without keyId
                    assert.doesNotMatch(sseMD.masterKeyId, /^arn:scality:kms/);
                }
                if (bktConf.masterKeyId) {
                    // arn prefixed even if not prefixed in input
                    assert.doesNotMatch(sseMD.configuredMasterKeyId, /^arn:scality:kms/);
                    assert.doesNotMatch(sseS3.KMSMasterKeyID, /^arn:scality:kms/);
                }
            });
        }

        testCasesObj.forEach(objConf => it(`should have pre uploaded object with SSE ${objConf.name}`, async () => {
            const obj = bkt.objs[objConf.name];
            void await assertObjectSSEMigration(bkt.name, obj.name, objConf, obj, bktConf, bkt, null, obj.body);
        }));

        testCasesObj.forEach(objConf => describe(`object enc-obj-${objConf.name}`, () => {
            const obj = {
                name: `enc-obj-${objConf.name}`,
                kmsKeyInfo: null,
                kmsKey: null,
                body: `BODY(enc-obj-${objConf.name})`,
            };
            /** to be used as source of copy */
            let objForCopy;

            before(async () => {
                if (objConf.algo && objConf.masterKeyId) {
                    obj.kmsKeyInfo = await promisify(kms.createBucketKey)(bucketInfo, log);
                    obj.kmsKey = objConf.arnPrefix
                        ? obj.kmsKeyInfo.masterKeyArn
                        : obj.kmsKeyInfo.masterKeyId;
                }
                objForCopy = bkt.objs[objConf.name];
            });

            it(`should PutObject ${obj.name} overriding bucket SSE`, async () => {
                void await putEncryptedObject(bkt.name, obj.name, objConf, obj.kmsKey, obj.body);
                // void await assertObjectSSE(bkt.name, obj.name, objConf, obj, bktConf, bkt, null, obj.body);
            });

            // TODO S3C-9996 Fix MPU & SSE to unskip this
            // Should as well fix the output of CreateMPU and UploadPart to include the SSE
            // and validate the SSE output here and check with listPart & listMultipartUploads as well
            const optionalSkip = objConf.algo || bktConf.masterKeyId || (!bktConf.algo && !bktConf.deleteSSE)
                ? it.skip
                : it;
            optionalSkip('should migrate completed MPU', async () => {
                const mpuKey = `${obj.name}-mpu`;

                const fullBody = `${obj.body}-MPU1${obj.body}-MPU2`;
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            optionalSkip('should migrate completed MPU that had copy', async () => {
                const mpuKey = `${obj.name}-mpucopy`;
                const fullBody = `BODY(copy)${obj.body}-MPU2`;
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            optionalSkip('should migrate completed MPU that had byte range copy', async () => {
                const mpuKey = `${obj.name}-mpucopyrange`;
                const fullBody = 'copyBODY';
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });
            const mpus = {};
            before('retrieve MPUS', async () => {
                const listed = await s3.listMultipartUploads({ Bucket: bkt.name }).promise();
                assert.strictEqual(listed.IsTruncated, false, 'Too much MPUs, need to loop on pagination');
                for (const mpu of listed.Uploads) {
                    mpus[mpu.Key] = mpu.UploadId;
                }
            });

            optionalSkip('should prepare empty encrypted MPU without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpu-empty`;
                const uploadId = mpus[mpuKey];
                assert(uploadId, 'Missing MPU, it should have been prepared before');

                const existingParts = await s3.listParts({
                    Bucket: bkt.name, Key: mpuKey, UploadId: uploadId }).promise();
                const partCount = (existingParts.Parts || []).length || 0;
                assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
                assert.strictEqual(partCount, 0);

                const part1 = await s3.uploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU1`,
                    Key: mpuKey,
                    PartNumber: 1,
                }).promise();
                // console.log('part1', part1);
                const part2 = await s3.uploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: 2,
                }).promise();
                // console.log('part2', part2);
                const complete = await s3.completeMultipartUpload({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    MultipartUpload: {
                        Parts: [
                            { PartNumber: 1, ETag: part1.ETag },
                            { PartNumber: 2, ETag: part2.ETag },
                        ],
                    },
                }).promise();
                // console.log('complete', complete);
                const fullBody = `${obj.body}-MPU1${obj.body}-MPU2`;
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            optionalSkip('should prepare encrypte MPU and put 2 encrypted parts without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpu`;
                const uploadId = mpus[mpuKey];
                assert(uploadId, 'Missing MPU, it should have been prepared before');

                const existingParts = await s3.listParts({
                    Bucket: bkt.name, Key: mpuKey, UploadId: uploadId }).promise();
                const partCount = (existingParts.Parts || []).length || 0;
                assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
                assert.strictEqual(partCount, 2);
                // console.log('existingParts', existingParts);

                const part1 = await s3.uploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU1`,
                    Key: mpuKey,
                    PartNumber: partCount + 1,
                }).promise();
                // console.log('part1', part1);
                const part2 = await s3.uploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: partCount + 2,
                }).promise();
                // console.log('part2', part2);
                const complete = await s3.completeMultipartUpload({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    MultipartUpload: {
                        Parts: [
                            ...existingParts.Parts.map(part => ({ PartNumber: part.PartNumber, ETag: part.ETag })),
                            { PartNumber: partCount + 1, ETag: part1.ETag },
                            { PartNumber: partCount + 2, ETag: part2.ETag },
                        ],
                    },
                }).promise();
                // console.log('complete', complete);
                const fullBody = `${obj.body}-MPU1${obj.body}-MPU2`.repeat(2);
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            optionalSkip('should prepare encrypted MPU and copy an encrypted parts from encrypted bucket without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpucopy`;
                const uploadId = mpus[mpuKey];
                assert(uploadId, 'Missing MPU, it should have been prepared before');

                const existingParts = await s3.listParts({
                    Bucket: bkt.name, Key: mpuKey, UploadId: uploadId }).promise();
                const partCount = (existingParts.Parts || []).length || 0;
                assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
                assert.strictEqual(partCount, 2);
                // console.log('existingParts', existingParts);

                const part1 = await s3.uploadPartCopy({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: partCount + 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                }).promise();
                // console.log('part1', part1);
                const part2 = await s3.uploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: partCount + 2,
                }).promise();
                // console.log('part2', part2);

                const complete = await s3.completeMultipartUpload({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    MultipartUpload: {
                        Parts: [
                            ...existingParts.Parts.map(part => ({ PartNumber: part.PartNumber, ETag: part.ETag })),
                            { PartNumber: partCount + 1, ETag: part1.ETag },
                            { PartNumber: partCount + 2, ETag: part2.ETag },
                        ],
                    },
                }).promise();
                // console.log('complete', complete);
                const fullBody = `BODY(copy)${obj.body}-MPU2`.repeat(2);
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            optionalSkip('should prepare encrypte MPU and copy an encrypted range parts from encrypted bucket without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpucopyrange`;
                const uploadId = mpus[mpuKey];
                assert(uploadId, 'Missing MPU, it should have been prepared before');

                const existingParts = await s3.listParts({
                    Bucket: bkt.name, Key: mpuKey, UploadId: uploadId }).promise();
                const partCount = (existingParts.Parts || []).length || 0;
                assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
                assert.strictEqual(partCount, 2);

                // source body is "BODY(copy)"
                const part1 = await s3.uploadPartCopy({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: partCount + 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                    CopySourceRange: 'bytes=5-8', // copy
                }).promise();
                // console.log('part1', part1);
                const part2 = await s3.uploadPartCopy({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: partCount + 2,
                    CopySource: `${copyBkt}/${copyObj}`,
                    CopySourceRange: 'bytes=0-3', // BODY
                }).promise();
                // console.log('part2', part2);
                const complete = await s3.completeMultipartUpload({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    MultipartUpload: {
                        Parts: [
                            ...existingParts.Parts.map(part => ({ PartNumber: part.PartNumber, ETag: part.ETag })),
                            { PartNumber: partCount + 1, ETag: part1.ETag },
                            { PartNumber: partCount + 2, ETag: part2.ETag },
                        ],
                    },
                }).promise();
                // console.log('complete', complete);
                const fullBody = 'copyBODY'.repeat(2);
                void await assertObjectSSEMigration(bkt.name, mpuKey, objConf, obj, bktConf, bkt, null, fullBody);
            });

            it(`should CopyObject ${obj.name} into encrypted destination bucket`, async () => {
                // if SSE not provided it uses bucket SSE
                const source = `${bkt.name}/${objForCopy.name}`;
                const copied = await s3.copyObject({
                    Bucket: copyBkt,
                    Key: source,
                    CopySource: source,
                }).promise();

                assert.strictEqual(copied.ServerSideEncryption, 'aws:kms');
                // TODO FIX return SSEKMSKeyId on CopyObject and test it

                const head = await s3.headObject({ Bucket: copyBkt, Key: source }).promise();
                // hardcoded SSE for copy bucket
                assert.strictEqual(head.ServerSideEncryption, 'aws:kms');
                if (config.kmsHideScalityArn) {
                    assert.doesNotMatch(head.SSEKMSKeyId, /^arn:scality:kms/);
                } else {
                    assert.match(head.SSEKMSKeyId, /^arn:scality:kms/);
                }

                const get = await s3.getObject({ Bucket: copyBkt, Key: source }).promise();
                assert.strictEqual(get.Body.toString(), objForCopy.body);
            });

            it(`should CopyObject ${obj.name} into same bucket with SSE config`, async () => {
                // if SSE not provided it uses bucket SSE
                const copyKey = `${obj.name}-copy`;
                const copied = await s3.copyObject({
                    ...putObjParams(bkt.name, copyKey, objConf, obj.kmsKey),
                    CopySource: `${bkt.name}/${obj.name}`,
                }).promise();
                // console.log('copied', copied);

                assert.strictEqual(copied.ServerSideEncryption, (objConf.algo || bktConf.algo));
                // TODO FIX return SSEKMSKeyId on CopyObject and test it

                void await assertObjectSSEMigration(bkt.name, copyKey, objConf, obj, bktConf, bkt, null, obj.body);
            });

            it(`should CopyObject from encrypted destination into ${obj.name}`, async () => {
                // if SSE not provided it uses bucket SSE
                const source = `${copyBkt}/${copyObj}`;
                const copyKey = `${obj.name}-copy-from`;
                const copied = await s3.copyObject({
                    ...putObjParams(bkt.name, copyKey, objConf, obj.kmsKey),
                    CopySource: source,
                }).promise();
                // console.log('copied', copied);

                assert.strictEqual(copied.ServerSideEncryption, (objConf.algo || bktConf.algo));
                // TODO FIX return SSEKMSKeyId on CopyObject and test it

                void await assertObjectSSEMigration(bkt.name, copyKey, objConf, obj, bktConf, bkt, null, 'BODY(copy)');
            });

            // it(`should PutObject versioned with SSE ${obj.name}`, async () => {
            //     // ensure versioned bucket is empty
            //     void await bucketUtil.empty(bkt.vname);
            //     let { Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise();
            //     // regularly count versioned objects
            //     assert.strictEqual(Versions.length, 0);

            //     const bodyBase = `BODY(${obj.name})-base`;
            //     void await putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyBase);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, null, bodyBase);
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 1);

            //     void await s3.putBucketVersioning({ Bucket: bkt.vname,
            //         VersioningConfiguration: { Status: 'Enabled' },
            //     }).promise();

            //     const bodyV1 = `BODY(${obj.name})-v1`;
            //     const v1 = await putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyV1);
            //     const bodyV2 = `BODY(${obj.name})-v2`;
            //     const v2 = await putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyV2);
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 3);

            //     const current = await s3.headObject({ Bucket: bkt.vname, Key: obj.name }).promise();
            //     assert.strictEqual(current.VersionId, v2.VersionId); // ensure versioning as expected
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 3);

            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, null, bodyV2); // v2
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, 'null', bodyBase);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, v1.VersionId, bodyV1);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, v2.VersionId, bodyV2);
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 3);

            //     void await s3.putBucketVersioning({ Bucket: bkt.vname,
            //         VersioningConfiguration: { Status: 'Suspended' },
            //     }).promise();

            //     // should be fine after version suspension
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, null, bodyV2); // v2
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, 'null', bodyBase);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, v1.VersionId, bodyV1);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, v2.VersionId, bodyV2);
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 3);

            //     // put a new null version
            //     const bodyFinal = `BODY(${obj.name})-final`;
            //     void await putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyFinal);
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, null, bodyFinal); // null
            //     void await assertObjectSSE(bkt.vname, obj.name, objConf, obj, bktConf, bkt, 'null', bodyFinal);
            //     ({ Versions } = await s3.listObjectVersions({ Bucket: bkt.vname }).promise());
            //     assert.strictEqual(Versions.length, 3);
            // });
        }));
    }));

    it('should prepare encrypted MPU and copy parts from all bkt and objects matrice without completion', async () => {
        const mpuKey = 'mpucopy';
        const listed = await s3.listMultipartUploads({ Bucket: mpuCopyBkt }).promise();
        assert.strictEqual(listed.IsTruncated, false, 'Too much MPUs, need to loop on pagination');
        assert.strictEqual(listed.Uploads.length, 1, 'There should be only one MPU for global copy');
        const uploadId = listed.Uploads[0].UploadId;
        const copyPartArg = {
            UploadId: uploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
        };

        const existingParts = await s3.listParts(copyPartArg).promise();
        const partCount = (existingParts.Parts || []).length || 0;
        assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
        assert.strictEqual(partCount, testCases.length * testCasesObj.length);

        // For each test Case bucket and object copy a part
        const uploadPromises = testCases.reduce((acc, bktConf, bktIdx) => {
            const bkt = bkts[bktConf.name];

            return acc.concat(testCasesObj.map(async (objConf, objIdx) => {
                const obj = bkt.objs[objConf.name];

                const partNumber = partCount + bktIdx * testCasesObj.length + objIdx + 1;
                const res = await s3.uploadPartCopy({
                    ...copyPartArg,
                    PartNumber: partNumber,
                    CopySource: `${bkt.name}/${obj.name}`,
                }).promise();

                return { partNumber, body: obj.body, res: res.CopyPartResult };
            }));
        }, []);

        const parts = await Promise.all(uploadPromises);
        // console.log('parts', parts);

        const complete = await s3.completeMultipartUpload({
            UploadId: uploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
            MultipartUpload: {
                Parts: [
                    ...existingParts.Parts.map(part => ({ PartNumber: part.PartNumber, ETag: part.ETag })),
                    ...parts.map(part => ({ PartNumber: part.partNumber, ETag: part.res.ETag })),
                ],
            },
        }).promise();
        // console.log('complete', complete);
        const fullBody = parts.reduce((acc, part) => `${acc}${part.body}`, '').repeat(2);
        void await assertObjectSSEMigration(mpuCopyBkt, mpuKey, {}, {}, { algo: 'AES256' }, {}, null, fullBody);
    });
});
