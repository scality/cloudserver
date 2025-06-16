/* eslint-disable */
const { promisify } = require('util');
const { DummyRequestLogger } = require('../../unit/helpers');
const assert = require('assert');
const log = new DummyRequestLogger();
const { makeRequest } = require('../raw-node/utils/makeRequest');
const helpers = require('./helpers');
const scenarios = require('./scenarios');

// copy part of aws-node-sdk/test/object/encryptionHeaders.js and add more tests

describe('SSE KMS arnPrefix', () => {
    /** Bucket to test CopyObject from and to */
    const copyBkt = 'enc-bkt-copy';
    const copyObj = 'copy-obj';
    let copyKmsKey;
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
            bkt.kmsKeyInfo = await helpers.createKmsKey(log);
            bkt.kmsKey = bktConf.arnPrefix
                ? bkt.kmsKeyInfo.masterKeyArn
                : bkt.kmsKeyInfo.masterKeyId;
        }
        void await helpers.s3.createBucket(({ Bucket: bkt.name })).promise();
        void await helpers.s3.createBucket(({ Bucket: bkt.vname })).promise();
        if (bktConf.deleteSSE) {
            await scenarios.deleteBucketSSEBeforeEach(bkt.name, log);
            await scenarios.deleteBucketSSEBeforeEach(bkt.vname, log);
        }
        if (bktConf.algo) {
            // bucket encryption will be asserted in bucket test
            void await helpers.s3.putBucketEncryption({
                Bucket: bkt.name,
                ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                    algo: bktConf.algo, masterKeyId: bkt.kmsKey }),
            }).promise();
            void await helpers.s3.putBucketEncryption({
                Bucket: bkt.vname,
                ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                    algo: bktConf.algo, masterKeyId: bkt.kmsKey }),
            }).promise();
        }

        // Put an object for each SSE conf in each bucket
        void await Promise.all(scenarios.testCases.map(async objConf => {
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

            await helpers.putEncryptedObject(bkt.name, obj.name, objConf, obj.kmsKey, obj.body);
        }));
    };

    before('setup', async () => {
        console.log('Run arnPrefix',
            { profile: helpers.credsProfile, accessKeyId: helpers.s3.config.credentials.accessKeyId });
        const allBuckets = (await helpers.s3.listBuckets().promise()).Buckets.map(b => b.Name);
        console.log('List buckets:', allBuckets);
        void await helpers.MD.setup();
        copyKmsKey = (await helpers.createKmsKey(log)).masterKeyArn;
        try {
            // pre cleanup
            void await cleanup(copyBkt);
            void await cleanup(mpuCopyBkt);
            void await Promise.all(Object.values(bkts).map(async bkt => {
                void await cleanup(bkt.name);
                return await cleanup(bkt.vname);
            }));
        } catch (e) { void e; }

        // init copy bucket
        void await helpers.s3.createBucket(({ Bucket: copyBkt })).promise();
        void await helpers.s3.createBucket(({ Bucket: mpuCopyBkt })).promise();
        void await helpers.s3.putBucketEncryption({
            Bucket: copyBkt,
            ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({ algo: 'aws:kms', masterKeyId: copyKmsKey }),
        }).promise();
        void await helpers.s3.putObject({ Bucket: copyBkt, Key: copyObj, Body: 'BODY(copy)' }).promise();

        // Prepare every buckets with 1 object (for copy)
        void await Promise.all(scenarios.testCases.map(async bktConf => this.initBucket(bktConf)));
    });

    after(async () => {
        void await helpers.cleanup(copyBkt);
        void await helpers.cleanup(mpuCopyBkt);
        // Clean every bucket
        void await Promise.all(Object.values(bkts).map(async bkt => {
            void await helpers.cleanup(bkt.name);
            return await helpers.cleanup(bkt.vname);
        }));
    });

    scenarios.testCases.forEach(bktConf => describe(`bucket enc-bkt-${bktConf.name}`, () => {
        let bkt = bkts[bktConf.name];

        before(() => {
            bkt = bkts[bktConf.name];
        });

        if (bktConf.deleteSSE) {
            beforeEach(async () => {
                await scenarios.deleteBucketSSEBeforeEach(bkt.name, log);
                await scenarios.deleteBucketSSEBeforeEach(bkt.vname, log);
            });
        }

        if (!bktConf.algo) {
            if (!bktConf.deleteSSE && helpers.config.globalEncryptionEnabled) {
                it('GetBucketEncryption should return AES256 because of globalEncryptionEnabled',
                    async () => await scenarios.tests.getBucketSSE(bkt.name, log, 'AES256', null, 'after'));
            } else {
                it('GetBucketEncryption should return ServerSideEncryptionConfigurationNotFoundError',
                    async () => await scenarios.tests.getBucketSSEError(bkt.name));
                if (!bktConf.deleteSSE) {
                    it('should have non mandatory SSE in bucket MD as test init put an object with AES256',
                        async () => await scenarios.tests.getBucketNonMandatorySSE(bkt.name, log, 'after'));
                }
            }
        } else {
            it('GetBucketEncryption should return SSE with arnPrefix to key',
                async () => await scenarios.tests.getBucketSSE(bkt.name, log, bktConf.algo,
                    bktConf.masterKeyId ? bkt.kmsKeyInfo.masterKeyArn : null, 'after'));
        }

        scenarios.testCasesObj.forEach(objConf => it(`should assert uploaded objects with SSE ${objConf.name}`,
            async () => scenarios.tests.getPreUploadedObject(bkt.name,
                { objConf, obj: bkt.objs[objConf.name] }, { bktConf, bkt }, 'after')));

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

            it(`should PutObject ${obj.name} overriding bucket SSE`,
                async () => scenarios.tests.putObjectOverrideSSE({ objConf, obj }, { bktConf, bkt }, 'after'));

            // CopyObject scenarios
            [
                { name: `${obj.name} into encrypted destination bucket`, forceBktSSE: true },
                { name: `${obj.name} into same bucket with object SSE config` },
                { name: `from encrypted source into ${obj.name} with object SSE config` }
            ].forEach(({ name, forceBktSSE }, index) =>
                it(`should CopyObject ${name}`, async () => 
                    await scenarios.tests.copyObjectAndSSE(
                        { copyBkt, objForCopy, copyObj},
                        { objConf, obj },
                        { bktConf, bkt },
                        { index, forceBktSSE },
                        'after',
                    )));

            // after SSE migration implementation all mpu with sse are fixed
            it('should encrypt MPU and put 2 encrypted parts', async () => {
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
                    }, mpu, objConf.algo || bktConf.algo, 'after');
                    newParts.push(part);
                }
                void await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts },
                    mpu, objConf.algo || bktConf.algo, 'after');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: `${obj.body}-MPU1${obj.body}-MPU2`,
                };
                void await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt }, {}, 'after');
            });

            it('should encrypt MPU and copy an encrypted parts from encrypted bucket', async () => {
                const mpuKey = `${obj.name}-mpucopy`;
                const mpu = await helpers.s3.createMultipartUpload(
                    helpers.putObjParams(bkt.name, mpuKey, objConf, obj.kmsKey)).promise();
                const part1 = await scenarios.tests.mpuUploadPartCopy({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Key: mpuKey,
                    PartNumber: 1,
                    CopySource: `${copyBkt}/${copyObj}`,
                }, mpu, objConf.algo || bktConf.algo, 'after');
                const part2 = await scenarios.tests.mpuUploadPart({
                    UploadId: mpu.UploadId,
                    Bucket: bkt.name,
                    Body: `${obj.body}-MPU2`,
                    Key: mpuKey,
                    PartNumber: 2,
                }, mpu, objConf.algo || bktConf.algo, 'after');

                void await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts: [part1, part2] },
                    mpu, objConf.algo || bktConf.algo, 'after');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: `BODY(copy)${obj.body}-MPU2`,
                };
                void await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt }, {}, 'after');
            });

            it('should encrypt MPU and copy an encrypted range parts from encrypted bucket', async () => {
                const mpuKey = `${obj.name}-mpucopy`;
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
                    }, mpu, objConf.algo || bktConf.algo, 'after');
                    newParts.push(part);
                }

                void await scenarios.tests.mpuComplete(
                    { UploadId: mpu.UploadId, Bucket: bkt.name, Key: mpuKey },
                    { existingParts: [], newParts },
                    mpu, objConf.algo || bktConf.algo, 'after');
                const assertion = {
                    Bucket: bkt.name,
                    Key: mpuKey,
                    Body: 'copyBODY',
                };
                void await scenarios.assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt }, {}, 'after');
            });

            it(`should PutObject versioned with SSE ${obj.name}`, async () => {
                // ensure versioned bucket is empty
                void await helpers.bucketUtil.empty(bkt.vname);
                let { Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise();
                // regularly count versioned objects
                assert.strictEqual(Versions.length, 0);

                const bodyBase = `BODY(${obj.name})-base`;
                void await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyBase);
                const baseAssertion = { Bucket: bkt.vname, Key: obj.name };
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyBase },
                    { objConf, obj }, { bktConf, bkt }, {}, 'after');
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 1);

                void await helpers.s3.putBucketVersioning({ Bucket: bkt.vname,
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

                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after'); // v2
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: 'null', Body: bodyBase }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v1.VersionId, Body: bodyV1 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v2.VersionId, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                void await helpers.s3.putBucketVersioning({ Bucket: bkt.vname,
                    VersioningConfiguration: { Status: 'Suspended' },
                }).promise();

                // should be fine after version suspension
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after'); // v2
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: 'null', Body: bodyBase }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v1.VersionId, Body: bodyV1 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, VersionId: v2.VersionId, Body: bodyV2 }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after');
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);

                // put a new null version
                const bodyFinal = `BODY(${obj.name})-final`;
                void await helpers.putEncryptedObject(bkt.vname, obj.name, objConf, obj.kmsKey, bodyFinal);
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyFinal }, { objConf, obj }, { bktConf, bkt },
                    {}, 'after'); // null
                void await scenarios.assertObjectSSE(
                    { ...baseAssertion, Body: bodyFinal }, { objConf, obj }, { bktConf, bkt },
                    'null', 'after');
                ({ Versions } = await helpers.s3.listObjectVersions({ Bucket: bkt.vname }).promise());
                assert.strictEqual(Versions.length, 3);
            });
        }));
    }));

    it('should encrypt MPU and copy parts from every buckets and objects matrice', async () => {
        void await helpers.s3.putBucketEncryption({
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

        const parts = await Promise.all(uploadPromises);

        void await helpers.s3.completeMultipartUpload({
            UploadId: mpu.UploadId,
            Bucket: mpuCopyBkt,
            Key: mpuKey,
            MultipartUpload: {
                Parts: parts.map(part => ({ PartNumber: part.partNumber, ETag: part.res.ETag })),
            },
        }).promise();
        const assertion = {
            Bucket: mpuCopyBkt,
            Key: mpuKey,
            Body: parts.reduce((acc, part) => `${acc}${part.body}`, ''),
        };
        void await scenarios.assertObjectSSE(assertion, { objConf: {}, obj: {} },
            { bktConf: { algo: 'AES256' }, bkt: {} }, {}, 'after');
    });
});

describe('ensure MPU use good SSE', () => {
    const mpuKmsBkt = 'bkt-mpu-kms';
    let kmsKeympuKmsBkt;

    before(async () => {
        kmsKeympuKmsBkt = (await helpers.createKmsKey(log)).masterKeyArn;
        void await helpers.MD.setup();
        void await helpers.s3.createBucket({ Bucket: mpuKmsBkt }).promise();
        void await helpers.s3.putBucketEncryption({
            Bucket: mpuKmsBkt,
            ServerSideEncryptionConfiguration:
                helpers.hydrateSSEConfig({ algo: 'aws:kms', masterKeyId: kmsKeympuKmsBkt }) }).promise();
    });

    after(async () => {
        void await helpers.cleanup(mpuKmsBkt);
    });

    it('mpu upload part should fail with sse header', async () => {
        const key = 'mpuKeyBadUpload';
        const mpu = await helpers.s3.createMultipartUpload({
            Bucket: mpuKmsBkt, Key: key }).promise();
        void await assert.rejects(promisify(makeRequest)({
            method: 'PUT',
            hostname: helpers.s3.endpoint.hostname,
            port: helpers.s3.endpoint.port,
            path: `/${mpuKmsBkt}/${key}`,
            headers: {
                'content-length': 4,
                // not allowed on UploadPart
                'x-amz-server-side-encryption': 'aws:kms',
                'x-amz-server-side-encryption-aws-kms-key-id': 'makeRequest',
            },
            queryObj: {
                uploadId: mpu.UploadId,
                partNumber: '2',
            },
            authCredentials: {
                accessKey: helpers.s3.config.credentials.accessKeyId,
                secretKey: helpers.s3.config.credentials.secretAccessKey,
            },
            requestBody: 'hello',
        }), err => {
            assert.strictEqual(err.code, 'InvalidArgument');
            return true;
        });
    });

    it('mpu should use encryption from createMPU', async () => {
        const key = 'mpuKey';
        const mpuKms = (await helpers.createKmsKey(log)).masterKeyArn;
        const mpu = await helpers.s3.createMultipartUpload({
            Bucket: mpuKmsBkt, Key: key, ServerSideEncryption: 'aws:kms', SSEKMSKeyId: mpuKms }).promise();
        assert.strictEqual(mpu.ServerSideEncryption, 'aws:kms');
        assert.strictEqual(mpu.SSEKMSKeyId, helpers.getKey(mpuKms));

        const part1 = await scenarios.tests.mpuUploadPart({
            UploadId: mpu.UploadId,
            Bucket: mpuKmsBkt,
            Body: 'Scality',
            Key: key,
            PartNumber: 1,
        }, mpu, 'aws:kms', 'after');
        await scenarios.tests.mpuComplete(
            { UploadId: mpu.UploadId, Bucket: mpuKmsBkt, Key: key },
            { existingParts: [], newParts: [part1] },
            mpu, 'aws:kms', 'after');

        const assertion = {
            Bucket: mpuKmsBkt,
            Key: key,
            Body: 'Scality',
        };
        const objForAssert = {
            objConf: { algo: 'aws:kms', masterKeyId: true },
            obj: { kmsKey: mpuKms, kmsKeyInfo: { masterKeyId: mpuKms, masterKeyArn: mpuKms } },
        };
        const bktForAssert = {
            bktConf: { algo: 'aws:kms', masterKeyId: true },
            bkt: { kmsKey: kmsKeympuKmsBkt,
                kmsKeyInfo: { masterKeyId: kmsKeympuKmsBkt, masterKeyArn: kmsKeympuKmsBkt } }
        };
        void await scenarios.assertObjectSSE(
            assertion,
            objForAssert,
            bktForAssert,
            {},
            'after',
        );
    });
});
