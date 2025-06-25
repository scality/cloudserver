/* eslint-disable no-console */
const kms = require('../../../lib/kms/wrapper');
const { promisify } = require('util');
const { DummyRequestLogger } = require('../../unit/helpers');
const assert = require('assert');
const metadata = require('../../../lib/metadata/wrapper');
const crypto = require('crypto');
const log = new DummyRequestLogger();
const helpers = require('./helpers');
const scenarios = require('./scenarios');

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests

// Fix for before migration run to not add a prefix
Object.defineProperty(kms, 'arnPrefix', { get() { return ''; } });

describe('SSE KMS before migration', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const copyObj = 'copy-obj';
    const copyKmsKey = `${kms.arnPrefix}${crypto.randomBytes(32).toString('hex')}`;
    const bkts = {};
    const mpuCopyBkt = 'enc-bkt-mpu-copy';

    this.initBucket = async function initBucket(bktConf) {
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
            const key = crypto.randomBytes(32).toString('hex');
            bkt.kmsKeyInfo = { masterKeyId: key, masterKeyArn: `${kms.arnPrefix}${key}` };
            bkt.kmsKey = bktConf.arnPrefix
                ? bkt.kmsKeyInfo.masterKeyArn
                : bkt.kmsKeyInfo.masterKeyId;
        }
        await helpers.s3.createBucket(({ Bucket: bkt.name })).promise();
        await helpers.s3.createBucket(({ Bucket: bkt.vname })).promise();
        if (bktConf.algo) {
            // bucket encryption will be asserted in bucket test
            await helpers.s3.putBucketEncryption({
                Bucket: bkt.name,
                ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                    algo: bktConf.algo, masterKeyId: bkt.kmsKey }),
            }).promise();
            await helpers.s3.putBucketEncryption({
                Bucket: bkt.vname,
                ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                    algo: bktConf.algo, masterKeyId: bkt.kmsKey }),
            }).promise();
        }

        // Put an object for each SSE conf in each bucket
        await Promise.all(scenarios.testCases.map(async objConf => {
            const obj = {
                name: `for-copy-enc-obj-${objConf.name}`,
                kmsKeyInfo: null,
                kmsKey: null,
                body: `BODY(for-copy-enc-obj-${objConf.name})`,
            };
            bkt.objs[objConf.name] = obj;
            if (objConf.algo && objConf.masterKeyId) {
                const key = crypto.randomBytes(32).toString('hex');
                obj.kmsKeyInfo = { masterKeyId: key, masterKeyArn: `${kms.arnPrefix}${key}` };
                obj.kmsKey = objConf.arnPrefix
                    ? obj.kmsKeyInfo.masterKeyArn
                    : obj.kmsKeyInfo.masterKeyId;
            }
            return await helpers.putEncryptedObject(bkt.name, obj.name, objConf, obj.kmsKey, obj.body);
        }));
    };

    before(async () => {
        console.log('Run before migration',
            { profile: helpers.credsProfile, accessKeyId: helpers.s3.config.credentials.accessKeyId });
        const allBuckets = (await helpers.s3.listBuckets().promise()).Buckets.map(b => b.Name);
        console.log('List buckets:', allBuckets);
        await promisify(metadata.setup.bind(metadata))();

        // init copy bucket
        await helpers.s3.createBucket(({ Bucket: copyBkt })).promise();
        await helpers.s3.createBucket(({ Bucket: mpuCopyBkt })).promise();
        await helpers.s3.putBucketEncryption({
            Bucket: copyBkt,
            ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({ algo: 'aws:kms', masterKeyId: copyKmsKey }),
        }).promise();
        await helpers.s3.putObject({ Bucket: copyBkt, Key: copyObj, Body: 'BODY(copy)' }).promise();

        // Prepare every buckets with 1 object (for copy)
        await Promise.all(scenarios.testCases.map(async bktConf => this.initBucket(bktConf)));
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
                   async () => await scenarios.tests.getBucketNonMandatorySSE(bkt.name, log, 'before'));
            }
        } else {
            it('GetBucketEncryption should return SSE with arnPrefix to key',
                async () => await scenarios.tests.getBucketSSE(bkt.name, log, bktConf.algo,
                    bktConf.masterKeyId ? bkt.kmsKeyInfo.masterKeyArn : null, 'before'));
        }

        scenarios.testCasesObj.forEach(objConf => it(`should have pre uploaded object with SSE ${objConf.name}`,
            async () => scenarios.tests.getPreUploadedObject(bkt.name,
                { objConf, obj: bkt.objs[objConf.name] }, { bktConf, bkt })));

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
                    const key = crypto.randomBytes(32).toString('hex');
                    obj.kmsKeyInfo = { masterKeyId: key, masterKeyArn: `${kms.arnPrefix}${key}` };
                    obj.kmsKey = objConf.arnPrefix
                        ? obj.kmsKeyInfo.masterKeyArn
                        : obj.kmsKeyInfo.masterKeyId;
                }
                objForCopy = bkt.objs[objConf.name];
            });

            it(`should PutObject ${obj.name} overriding bucket SSE`,
                async () => scenarios.tests.putObjectOverrideSSE({ objConf, obj }, { bktConf, bkt }));

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
                        { index, forceBktSSE },
                        'before',
                    )));

            // S3C-9996 The SSE was bugged with MPU, where the completion takes only the masterKeyId from bucket
            // Fixed at the same time as migration, some scenario can pass only in newer version above migration
            const optionalSkip = objConf.algo || bktConf.masterKeyId || (!bktConf.algo && !bktConf.deleteSSE)
                ? it.skip
                : it;
            optionalSkip('should encrypt MPU and put 2 encrypted parts', async () => {
                const mpuKey = `${obj.name}-mpu`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                const partsBody = [`${obj.body}-MPU1`, `${obj.body}-MPU2`];
                const newParts = [];
                for (const [index, body] of partsBody.entries()) {
                    const part = await scenarios.tests.mpuUploadPart({
                        UploadId: mpu.UploadId,
                        Bucket: bkt.name,
                        Body: body,
                        Key: mpuKey,
                        PartNumber: index + 1,
                    }, mpu, objConf.algo || bktConf.algo, 'before');
                    newParts.push(part);
                }
                await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts },
                    mpu, objConf.algo || bktConf.algo, 'before');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: `${obj.body}-MPU1${obj.body}-MPU2`,
                };
                await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt });
            });

            optionalSkip('should encrypt MPU and copy an encrypted parts from encrypted bucket', async () => {
                const mpuKey = `${obj.name}-mpucopy`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                const part1 = await scenarios.tests.mpuUploadPartCopy({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                }, mpu, objConf.algo || bktConf.algo, 'before');
                const part2 = await scenarios.tests.mpuUploadPart({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: 2,
                }, mpu, objConf.algo || bktConf.algo, 'before');

                await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts: [part1, part2] },
                    mpu, objConf.algo || bktConf.algo, 'before');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: `BODY(copy)${obj.body}-MPU2`,
                };
                await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt });
            });

            optionalSkip('should encrypt MPU and copy an encrypted range parts from encrypted bucket', async () => {
                const mpuKey = `${obj.name}-mpucopyrange`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                // source body is "BODY(copy)"
                // [copy, BODY]
                const sourceRanges = ['bytes=5-8', 'bytes=0-3'];
                const newParts = [];
                for (const [index, range] of sourceRanges.entries()) {
                    const part = await scenarios.tests.mpuUploadPartCopy({
                        UploadId: mpu.UploadId,
                        Bucket: bkt.name,
                        Key: mpuKey,
                        PartNumber: index + 1,
                        CopySource: `${copyBkt}/${copyObj}`,
                        CopySourceRange: range,
                    }, mpu, objConf.algo || bktConf.algo, 'before');
                    newParts.push(part);
                }

                await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts },
                    mpu, objConf.algo || bktConf.algo, 'before');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: 'copyBODY',
                };
                await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt });
            });

            optionalSkip('should prepare empty encrypted MPU without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpu-empty`;
                await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
            });

            optionalSkip('should prepare encrypte MPU and put 2 encrypted parts without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpu`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                const partsBody = [`${obj.body}-MPU1`, `${obj.body}-MPU2`];
                for (const [index, body] of partsBody.entries()) {
                    await scenarios.tests.mpuUploadPart({
                        UploadId: mpu.UploadId,
                        Bucket: bkt.name,
                        Body: body,
                        Key: mpuKey,
                        PartNumber: index + 1,
                    }, mpu, objConf.algo || bktConf.algo, 'before');
                }
            });

            optionalSkip('should prepare encrypted MPU and copy an encrypted parts ' +
                'from encrypted bucket without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpucopy`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                await scenarios.tests.mpuUploadPartCopy({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                }, mpu, objConf.algo || bktConf.algo, 'before');
                await scenarios.tests.mpuUploadPart({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: 2,
                }, mpu, objConf.algo || bktConf.algo, 'before');
            });

            optionalSkip('should prepare encrypte MPU and copy an encrypted range parts ' +
                'from encrypted bucket without completion', async () => {
                const mpuKey = `${obj.name}-migration-mpucopyrange`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                // source body is "BODY(copy)"
                // [copy, BODY]
                const sourceRanges = ['bytes=5-8', 'bytes=0-3'];
                for (const [index, range] of sourceRanges.entries()) {
                    await scenarios.tests.mpuUploadPartCopy({
                        UploadId: mpu.UploadId,
                        Bucket: bkt.name,
                        Key: mpuKey,
                        PartNumber: index + 1,
                        CopySource: `${copyBkt}/${copyObj}`,
                        CopySourceRange: range,
                    }, mpu, objConf.algo || bktConf.algo, 'before');
                }
            });

            it(`should PutObject versioned with SSE ${obj.name}`, async () => {
                // ensure versioned bucket is empty
                await helpers.bucketUtil.empty(bkt.vname);
                let { Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise();
                // regularly count versioned objects
                assert.strictEqual(Versions.length, 0);

                const bodyBase = `BODY(${obj.name})-base`;
                await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyBase);
                const baseAssertion = { Bucket: bkt.vname, Key: obj.name };
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyBase },
                    { objConf, obj }, { bktConf, bkt });
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 1);

                await helpers.s3.putBucketVersioning({ Bucket: bkt.vname,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise();

                const bodyV1 = `BODY(${obj.name})-v1`;
                const v1 = await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyV1);
                const bodyV2 = `BODY(${obj.name})-v2`;
                const v2 = await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyV2);
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                const current = await helpers.s3.headObject({ Bucket: bkt.vname, Key: obj.name }).promise();
                assert.strictEqual(current.VersionId, v2.VersionId); // ensure versioning as expected
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt }); // v2
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: 'null', Body: bodyBase }, { objConf, obj }, { bktConf, bkt });
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v1.VersionId, Body: bodyV1 }, { objConf, obj }, { bktConf, bkt });
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v2.VersionId, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt });
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                await helpers.s3.putBucketVersioning({ Bucket: bkt.vname,
                    VersioningConfiguration: { Status: 'Suspended' },
                }).promise();

                // should be fine after version suspension
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt }); // v2
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: 'null', Body: bodyBase }, { objConf, obj }, { bktConf, bkt });
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v1.VersionId, Body: bodyV1 }, { objConf, obj }, { bktConf, bkt });
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v2.VersionId, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt });
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                // put a new null version
                const bodyFinal = `BODY(${obj.name})-final`;
                await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyFinal);
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyFinal }, { objConf, obj }, { bktConf, bkt }); // null
                await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyFinal }, { objConf, obj }, { bktConf, bkt }, 'null');
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);
            });
        }));
    }));

    it('should prepare encrypted MPU and copy parts from ' +
        'every buckets and objects matrice without completion', async () => {
        await helpers.s3.putBucketEncryption({
            Bucket: mpuCopyBkt,
            // AES256 because input key is broken for now
            ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({ algo: 'AES256' }),
        }).promise();
        const mpuKey = 'mpucopy';
        const mpu = await helpers.s3.createMultipartUpload(
            helpers.putObjParams(mpuCopyBkt, mpuKey, {}, null)).promise();
        const copyPartArg = {
            UploadId: mpu.UploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
        };
        // For each test Case bucket and object copy a part
        const uploadPromises = scenarios.testCases.reduce((acc, bktConf, bktIdx) => {
            const bkt = bkts[bktConf.name];

            return acc.concat(scenarios.testCasesObj.map(async (objConf, objIdx) => {
                const obj = bkt.objs[objConf.name];

                const partNumber = bktIdx * scenarios.testCasesObj.length + objIdx + 1;
                const res = await helpers.s3.uploadPartCopy({
                    ...copyPartArg,
                    PartNumber: partNumber,
                    CopySource: `${bkt.name}/${obj.name}`,
                }).promise();

                return { partNumber, body: obj.body, res: res.CopyPartResult };
            }));
        }, []);

        await Promise.all(uploadPromises);
    });
});
