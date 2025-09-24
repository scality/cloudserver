/* eslint-disable no-console */
const kms = require('../../../lib/kms/wrapper');
const filekms = require('../../../lib/kms/file/backend');
const { splitter, mpuBucketPrefix } = require('../../../constants');
const { DummyRequestLogger } = require('../../unit/helpers');
const assert = require('assert');
const log = new DummyRequestLogger();
const { isScalityKmsArn, SCAL_KMS_ARN } = require('arsenal/build/lib/network/KMSInterface');
const helpers = require('./helpers');
const scenarios = require('./scenarios');

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests

const fileKmsPrefix = filekms.backend.arnPrefix;
const fileArnPrefix = { arnPrefix: fileKmsPrefix };
const SCAL_KMS_ARN_REG = new RegExp(`^${SCAL_KMS_ARN}`);

async function assertObjectSSE(
    { Bucket, Key, VersionId, Body },
    { obj, objConf },
    { bkt, bktConf },
    // headers come from the command like putObject, CopyObject, MPUs...
    { arnPrefix = kms.arnPrefix, put, headers } = { arnPrefix: kms.arnPrefix },
) {
    const sseMD = await helpers.getObjectMDSSE(Bucket, Key);
    const head = await helpers.s3.headObject({ Bucket, Key, VersionId });
    const sseMDMigrated = await helpers.getObjectMDSSE(Bucket, Key);
    const expectedKey = `${sseMD.SSEKMSKeyId && isScalityKmsArn(sseMD.SSEKMSKeyId)
        ? '' : arnPrefix}${sseMD.SSEKMSKeyId}`;

    if (!put && sseMD.SSEKMSKeyId) {
        assert.doesNotMatch(sseMD.SSEKMSKeyId, SCAL_KMS_ARN_REG);
    }

    // obj precedence over bkt
    assert.strictEqual(head.ServerSideEncryption, (objConf.algo || bktConf.algo));
    headers && assert.strictEqual(headers.ServerSideEncryption, (objConf.algo || bktConf.algo));

    if (sseMDMigrated.SSEKMSKeyId) {
        // on metadata verify the full key with arn prefix
        assert.strictEqual(sseMDMigrated.SSEKMSKeyId, expectedKey);
    }

    if (obj.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(expectedKey));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(expectedKey));
    } else if (objConf.algo !== 'AES256' && bkt.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(expectedKey));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(expectedKey));
    } else if (head.ServerSideEncryption === 'aws:kms') {
        // We differ from aws behavior and always return a
        // masterKeyId even when not explicitly configured.
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(expectedKey));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(expectedKey));
    } else {
        assert.strictEqual(head.SSEKMSKeyId, undefined);
        headers && assert.strictEqual(headers.SSEKMSKeyId, undefined);
    }

    // always verify GetObject as well to ensure acurate decryption
    const get = await helpers.s3.getObject({ Bucket, Key, ...(VersionId && { VersionId }) });
    assert.strictEqual(get.Body.toString(), Body);
}

describe('SSE KMS migration', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const copyObj = 'copy-obj';
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
            bkt.kmsKeyInfo = await helpers.createKmsKey(log);
            bkt.kmsKey = bktConf.arnPrefix
                ? bkt.kmsKeyInfo.masterKeyArn
                : bkt.kmsKeyInfo.masterKeyId;
        }
        await helpers.s3.headBucket(({ Bucket: bkt.name }));
        await helpers.s3.headBucket(({ Bucket: bkt.vname }));
        if (bktConf.algo) {
            const bktSSE = await helpers.getBucketSSE(bkt.name);
            assert.strictEqual(bktSSE.SSEAlgorithm, bktConf.algo);
            if (bktSSE.KMSMasterKeyID) {
                assert.doesNotMatch(bktSSE.KMSMasterKeyID, SCAL_KMS_ARN_REG);
            }

            const vbktSSE = await helpers.getBucketSSE(bkt.vname);
            assert.strictEqual(vbktSSE.SSEAlgorithm, bktConf.algo);
            if (vbktSSE.KMSMasterKeyID) {
                assert.doesNotMatch(vbktSSE.KMSMasterKeyID, SCAL_KMS_ARN_REG);
            }
        }

        // Check object SSE using MD api, not S3 to avoid triggering migration
        await Promise.all(scenarios.testCases.map(async objConf => {
            const obj = {
                name: `for-copy-enc-obj-${objConf.name}`,
                kmsKeyInfo: null,
                kmsKey: null,
                body: `BODY(for-copy-enc-obj-${objConf.name})`,
            };
            bkt.objs[objConf.name] = obj;
            if (objConf.algo && objConf.masterKeyId) {
                obj.kmsKeyInfo = await helpers.createKmsKey(log);
                obj.kmsKey = objConf.arnPrefix
                    ? obj.kmsKeyInfo.masterKeyArn
                    : obj.kmsKeyInfo.masterKeyId;
            }
            const objSSE = await helpers.getObjectMDSSE(bkt.name, obj.name);
            assert.strictEqual(objSSE.ServerSideEncryption, objConf.algo || bktConf.algo || '');
            assert.doesNotMatch(objSSE.SSEKMSKeyId, SCAL_KMS_ARN_REG);
            return undefined;
        }));
    };

    before('setup', async () => {
        console.log('Run migration',
            { profile: helpers.credsProfile, accessKeyId: helpers.s3.config.credentials.accessKeyId });
        const allBuckets = (await helpers.s3.listBuckets()).Buckets.map(b => b.Name);
        console.log('List buckets:', allBuckets);
        await helpers.MD.setup();
        await helpers.s3.headBucket({ Bucket: copyBkt });
        await helpers.s3.headBucket(({ Bucket: mpuCopyBkt }));
        const copySSE = await helpers.s3.getBucketEncryption({ Bucket: copyBkt });
        const { SSEAlgorithm, KMSMasterKeyID } = copySSE
            .ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault;
        assert.strictEqual(SSEAlgorithm, 'aws:kms');
        assert.doesNotMatch(KMSMasterKeyID, SCAL_KMS_ARN_REG);

        // Check buckets and object are ready and not yet migrated
        await Promise.all(scenarios.testCases.map(async bktConf => this.checkInitBucket(bktConf)));
    });

    after(async () => {
        await helpers.cleanup(copyBkt);
        await helpers.cleanup(mpuCopyBkt);
        // Clean every bucket
        await Promise.all(Object.values(bkts).map(async bkt => {
            await helpers.cleanup(bkt.name);
            return await helpers.cleanup(bkt.vname);
        }));
    });

    scenarios.testCases.forEach(bktConf => describe(`bucket enc-bkt-${bktConf.name}`, () => {
        let bkt = bkts[bktConf.name];

        before(() => {
            bkt = bkts[bktConf.name];
        });

        if (bktConf.deleteSSE) {
            beforeEach(async () => scenarios.deleteBucketSSEBeforeEach(bkt.name, log));
        }

        if (!bktConf.algo) {
            it('GetBucketEncryption should return ServerSideEncryptionConfigurationNotFoundError',
                async () => await scenarios.tests.getBucketSSEError(bkt.name));

            if (!bktConf.deleteSSE) {
                it('should have non mandatory SSE in bucket MD as test init put an object with AES256',
                    async () => scenarios.tests.getBucketNonMandatorySSE(bkt.name, log, 'migration'));
            }
        } else {
            it('ensure old SSE KMS key setup',
                async () => await scenarios.tests.getBucketSSE(bkt.name, log, bktConf.algo,
                    bktConf.masterKeyId ? bkt.kmsKeyInfo.masterKeyArn : null, 'migration'));
        }

        scenarios.testCasesObj.forEach(objConf => it(`should have pre uploaded object with SSE ${objConf.name}`,
            async () => {
                const obj = bkt.objs[objConf.name];
                // use MD here to avoid triggering a migration
                const sseMD = await helpers.getObjectMDSSE(bkt.name, obj.name);
                if (sseMD.SSEKMSKeyId) {
                    assert.doesNotMatch(sseMD.SSEKMSKeyId, SCAL_KMS_ARN_REG);
                }
            }));

        scenarios.testCasesObj.forEach(objConf => describe(`object enc-obj-${objConf.name}`, () => {
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
                    obj.kmsKeyInfo = await helpers.createKmsKey(log);
                    obj.kmsKey = objConf.arnPrefix
                        ? obj.kmsKeyInfo.masterKeyArn
                        : obj.kmsKeyInfo.masterKeyId;
                }
                objForCopy = bkt.objs[objConf.name];
            });

            const mpus = {};
            before('retrieve MPUS', async () => {
                const listed = await helpers.s3.listMultipartUploads({ Bucket: bkt.name });
                assert.strictEqual(listed.IsTruncated, false, 'Too much MPUs, need to loop on pagination');
                for (const mpu of listed.Uploads) {
                    mpus[mpu.Key] = mpu.UploadId;
                }
            });

            it(`should PutObject ${obj.name} overriding bucket SSE`, async () => {
                await helpers.putEncryptedObject(bkt.name, obj.name, objConf, obj.kmsKey, obj.body);
                const assertion = {
                    Bucket: bkt.name,
                    Key: obj.name,
                    Body: obj.body,
                };
                await assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt }, { put: true });
            });

            // CopyObject scenarios
            [
                { name: `${obj.name} into encrypted destination bucket`, forceBktSSE: true },
                { name: `${obj.name} into same bucket with object SSE config` },
                { name: `from encrypted source into ${obj.name} with object SSE config` },
            ].forEach(({ name, forceBktSSE }, index) =>
                it(`should CopyObject ${name}`, async () =>
                    await scenarios.tests.copyObjectAndSSE(
                        { copyBkt, objForCopy, copyObj },
                        { objConf, obj },
                        { bktConf, bkt },
                        { index, forceBktSSE, assertObjectSSEFct: assertObjectSSE },
                    )));

            // S3C-9996 The SSE was bugged with MPU, where the completion takes only the masterKeyId from bucket
            // Fixed at the same time as migration, some scenario can pass only in newer version above migration
            const optionalSkip = objConf.algo || bktConf.masterKeyId || (!bktConf.algo && !bktConf.deleteSSE)
                ? it.skip
                : it;

            // completed MPU should behave like regular objects
            [
                { name: '', keySuffix: '', body: `${obj.body}-MPU1${obj.body}-MPU2` },
                { name: 'that has copy', keySuffix: 'copy', body: `BODY(copy)${obj.body}-MPU2` },
                { name: 'that has byte range copy', keySuffix: 'copyrange', body: 'copyBODY' },
            ].forEach(({ name, keySuffix, body }) =>
                optionalSkip(`should migrate completed MPU ${name}`, async () => {
                    const mpuKey = `${obj.name}-mpu${keySuffix}`;
                    const assertion = { Bucket: bkt.name, Key: mpuKey, Body: body };
                    await assertObjectSSE(
                        assertion, { objConf, obj }, { bktConf, bkt }, fileArnPrefix);
                }));

            async function prepareMPUTest(mpuKey, expectedExistingParts) {
                const uploadId = mpus[mpuKey];
                assert(uploadId, 'Missing MPU, it should have been prepared before');
                const MPUBucketName = `${mpuBucketPrefix}${bkt.name}`;
                const longMPUIdentifier = `overview${splitter}${mpuKey}${splitter}${uploadId}`;
                const mpuOverviewMDSSE = await helpers.getObjectMDSSE(MPUBucketName, longMPUIdentifier);

                const existingParts = await helpers.s3.listParts({
                    Bucket: bkt.name, Key: mpuKey, UploadId: uploadId });
                const partCount = (existingParts.Parts || []).length || 0;
                assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
                assert.strictEqual(partCount, expectedExistingParts);
                return { mpuKey, uploadId, mpuOverviewMDSSE, partCount, existingParts: existingParts.Parts || [] };
            }

            // ongoing MPU with regular uploadPart
            [
                {
                    name: 'empty',
                    keySuffix: '-empty',
                    existingPartsCount: 0,
                    partsBody: [`${obj.body}-MPU1`, `${obj.body}-MPU2`],
                    body: `${obj.body}-MPU1${obj.body}-MPU2`,
                },
                {
                    name: 'with 2 parts',
                    keySuffix: '',
                    existingPartsCount: 2,
                    partsBody: [`${obj.body}-MPU1`, `${obj.body}-MPU2`],
                    body: `${obj.body}-MPU1${obj.body}-MPU2`.repeat(2),
                },
            ].forEach(({ name, keySuffix, existingPartsCount, partsBody, body }) =>
                optionalSkip(`should finish ongoing encrypted MPU ${name} by adding 2 parts`, async () => {
                    const { mpuKey, uploadId, mpuOverviewMDSSE, existingParts, partCount } =
                        await prepareMPUTest(`${obj.name}-migration-mpu${keySuffix}`, existingPartsCount);
                    const newParts = [];
                    for (const [index, body] of partsBody.entries()) {
                        const part = await scenarios.tests.mpuUploadPart({
                            UploadId: uploadId,
                            Bucket: bkt.name,
                            Body: body,
                            Key: mpuKey,
                            PartNumber: partCount + index + 1,
                        }, mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                        newParts.push(part);
                    }
                    await scenarios.tests.mpuComplete(
                        { UploadId: uploadId, Bucket: bkt.name, Key: mpuKey },
                        { existingParts, newParts },
                        mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                    const assertion = {
                        Bucket: bkt.name,
                        Key: mpuKey,
                        Body: body,
                    };
                    await assertObjectSSE(
                        assertion, { objConf, obj }, { bktConf, bkt }, fileArnPrefix);
                }));

            optionalSkip('should finish ongoing encrypted MPU with 2 parts by copy and upload part', async () => {
                const { mpuKey, uploadId, mpuOverviewMDSSE, existingParts, partCount } =
                    await prepareMPUTest(`${obj.name}-migration-mpucopy`, 2);
                const part1 = await scenarios.tests.mpuUploadPartCopy({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: partCount + 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                }, mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                const part2 = await scenarios.tests.mpuUploadPart({
                    UploadId: uploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: partCount + 2,
                }, mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                await scenarios.tests.mpuComplete(
                    { UploadId: uploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts, newParts: [part1, part2] },
                    mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: `BODY(copy)${obj.body}-MPU2`.repeat(2),
                };
                await assertObjectSSE(
                    assertion, { objConf, obj }, { bktConf, bkt }, fileArnPrefix);
            });

            optionalSkip('should finish ongoing encrypted MPU with 2 parts by 2 copy byte range', async () => {
                const { mpuKey, uploadId, mpuOverviewMDSSE, existingParts, partCount } =
                    await prepareMPUTest(`${obj.name}-migration-mpucopyrange`, 2);
                // source body is "BODY(copy)"
                // [copy, BODY]
                const sourceRanges = ['bytes=5-8', 'bytes=0-3'];
                const newParts = [];
                for (const [index, range] of sourceRanges.entries()) {
                    const part = await scenarios.tests.mpuUploadPartCopy({
                        UploadId: uploadId,
                        Bucket: bkt.name,
                        Key: mpuKey,
                        PartNumber: partCount + index + 1,
                        CopySource: `${copyBkt}/${copyObj}`,
                        CopySourceRange: range,
                    }, mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                    newParts.push(part);
                }

                await scenarios.tests.mpuComplete(
                    { UploadId: uploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts, newParts },
                    mpuOverviewMDSSE, objConf.algo || bktConf.algo);
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: 'copyBODY'.repeat(2),
                };
                await assertObjectSSE(
                    assertion, { objConf, obj }, { bktConf, bkt }, fileArnPrefix);
            });
        }));
    }));

    it('should finish ongoing encrypted MPU by copy parts from all bkt and objects matrice', async () => {
        const mpuKey = 'mpucopy';
        const listed = await helpers.s3.listMultipartUploads({ Bucket: mpuCopyBkt });
        assert.strictEqual(listed.IsTruncated, false, 'Too much MPUs, need to loop on pagination');
        assert.strictEqual(listed.Uploads.length, 1, 'There should be only one MPU for global copy');
        const uploadId = listed.Uploads[0].UploadId;
        const copyPartArg = {
            UploadId: uploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
        };

        const existingParts = await helpers.s3.listParts(copyPartArg);
        const partCount = (existingParts.Parts || []).length || 0;
        assert.strictEqual(existingParts.IsTruncated, false, 'Too much parts, need to loop on pagination');
        assert.strictEqual(partCount, scenarios.testCases.length * scenarios.testCasesObj.length);

        // For each test Case bucket and object copy a part
        const uploadPromises = scenarios.testCases.reduce((acc, bktConf, bktIdx) => {
            const bkt = bkts[bktConf.name];

            return acc.concat(scenarios.testCasesObj.map(async (objConf, objIdx) => {
                const obj = bkt.objs[objConf.name];

                const partNumber = partCount + bktIdx * scenarios.testCasesObj.length + objIdx + 1;
                const res = await helpers.s3.uploadPartCopy({
                    ...copyPartArg,
                    PartNumber: partNumber,
                    CopySource: `${bkt.name}/${obj.name}`,
                });

                return { partNumber, body: obj.body, res: res.CopyPartResult };
            }));
        }, []);

        const parts = await Promise.all(uploadPromises);

        await helpers.s3.completeMultipartUpload({
            UploadId: uploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
            MultipartUpload: {
                Parts: [
                    ...existingParts.Parts.map(part => ({ PartNumber: part.PartNumber, ETag: part.ETag })),
                    ...parts.map(part => ({ PartNumber: part.partNumber, ETag: part.res.ETag })),
                ],
            },
        });
        const assertion = {
            Bucket: mpuCopyBkt,
            Key: mpuKey,
            Body: parts.reduce((acc, part) => `${acc}${part.body}`, '').repeat(2),
        };
        await assertObjectSSE(
            assertion, { objConf: {}, obj: {} }, { bktConf: { algo: 'AES256' }, bkt: {} }, fileArnPrefix);
    });
});
