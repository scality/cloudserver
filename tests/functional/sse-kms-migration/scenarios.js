const assert = require('assert');
const { getKeyIdFromArn, isScalityKmsArn, SCAL_KMS_ARN } = require('arsenal/build/lib/network/KMSInterface');
const helpers = require('./helpers');
const kms = require('../../../lib/kms/wrapper');

const SCAL_KMS_ARN_REG = new RegExp(`^${SCAL_KMS_ARN}`);
const arnPrefixReg = new RegExp(`^${kms.arnPrefix}`);

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

// assert object sse outside migration (before and after)
async function assertObjectSSE(
    { Bucket, Key, VersionId, Body },
    { obj, objConf },
    { bkt, bktConf },
    // headers come from the command like putObject, CopyObject, MPUs...
    { arnPrefix = kms.arnPrefix, headers } = { arnPrefix: kms.arnPrefix },
    testCase,
) {
    const head = await helpers.s3.headObject({ Bucket, Key, VersionId });
    const sseMD = await helpers.getObjectMDSSE(Bucket, Key);
    const arnPrefixReg = new RegExp(`^${arnPrefix}`);

    const expectedAlgo = (objConf.algo || bktConf.algo) ||
        (testCase === 'after' && helpers.config.globalEncryptionEnabled && !bktConf.deleteSSE
            ? 'AES256'
            : undefined);

    // obj precedence over bkt
    assert.strictEqual(head.ServerSideEncryption, expectedAlgo);
    headers && assert.strictEqual(headers.ServerSideEncryption, expectedAlgo);

    if (sseMD.SSEKMSKeyId) {
        // on metadata verify the full key with arn prefix
        assert.match(sseMD.SSEKMSKeyId, arnPrefixReg);
    }

    if (obj.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(obj.kmsKeyInfo.masterKeyArn));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(obj.kmsKeyInfo.masterKeyArn));
    } else if (objConf.algo !== 'AES256' && bkt.kmsKey) {
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(bkt.kmsKeyInfo.masterKeyArn));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(bkt.kmsKeyInfo.masterKeyArn));
    } else if (head.ServerSideEncryption === 'aws:kms') {
        // We differ from aws behavior and always return a
        // masterKeyId even when not explicitly configured.
        assert.strictEqual(head.SSEKMSKeyId, helpers.getKey(sseMD.SSEKMSKeyId));
        headers && assert.strictEqual(headers.SSEKMSKeyId, helpers.getKey(sseMD.SSEKMSKeyId));
    } else {
        assert.strictEqual(head.SSEKMSKeyId, undefined);
        headers && assert.strictEqual(headers.SSEKMSKeyId, undefined);
    }

    // always verify GetObject as well to ensure accurate decryption
    const get = await helpers.s3.getObject({ Bucket, Key, ...(VersionId && { VersionId }) });
    assert.strictEqual(get.Body.toString(), Body);
}

async function deleteBucketSSEBeforeEach(bktName, log) {
    const bucketMD = await helpers.MD.getBucket(bktName, log);
    if (bucketMD.getServerSideEncryption()) {
        bucketMD.setServerSideEncryption(null);
        await helpers.MD.updateBucket(bucketMD.getName(), bucketMD, log);
    }
}

async function getBucketSSEError(Bucket) {
    try {
        await helpers.s3.getBucketEncryption({ Bucket });
        throw new Error('Expected error but got success');
    } catch (err) {
        assert.strictEqual(err.name, 'ServerSideEncryptionConfigurationNotFoundError');
    }
}

// testCase should be one of before, migration, after
async function getBucketNonMandatorySSE(bktName, log, testCase) {
    const bucketMD = await helpers.MD.getBucket(bktName, log);
    const sseMD = bucketMD.getServerSideEncryption();
    assert.strictEqual(sseMD.mandatory, false);
    assert.strictEqual(sseMD.algorithm, 'AES256');
    if (testCase === 'after') {
        assert.match(sseMD.masterKeyId, arnPrefixReg);
    } else {
        assert.doesNotMatch(sseMD.masterKeyId, SCAL_KMS_ARN_REG);
    }
}

async function getBucketSSE(bktName, log, algo, masterKeyArn, testCase) {
    // bucket already has SSE from initBucket function
    const sseS3 = await helpers.getBucketSSE(bktName);
    // Compare bucketMD as well to make sure key is stored with arn
    const bucketMD = await helpers.MD.getBucket(bktName, log);
    const sseMD = bucketMD.getServerSideEncryption();

    assert.strictEqual(sseS3.SSEAlgorithm, algo);
    assert.strictEqual(sseMD.algorithm, algo);
    if (!masterKeyArn) {
        // AES256 or aws:kms without keyId
        testCase === 'after'
            ? assert.match(sseMD.masterKeyId, arnPrefixReg)
            : assert.doesNotMatch(sseMD.masterKeyId, SCAL_KMS_ARN_REG);
    }
    if (masterKeyArn) {
        // aws:kms with keyId
        if (testCase === 'migration') {
            // ensure key is old format
            assert.doesNotMatch(sseMD.configuredMasterKeyId, SCAL_KMS_ARN_REG);
            assert.doesNotMatch(sseS3.KMSMasterKeyID, SCAL_KMS_ARN_REG);
        } else {
            assert.strictEqual(sseMD.configuredMasterKeyId, masterKeyArn);
            assert.strictEqual(sseS3.KMSMasterKeyID, helpers.getKey(masterKeyArn));
        }
    }
}

// not for migration scenario
async function getPreUploadedObject(bktName, objAssert, bktAssert, testCase) {
    const assertion = {
        Bucket: bktName,
        Key: objAssert.obj.name,
        Body: objAssert.obj.body,
    };
    await assertObjectSSE(assertion, objAssert, bktAssert, {}, testCase);
}

// not for migration scenario
async function putObjectOverrideSSE({ objConf, obj }, { bktConf, bkt }, testCase) {
    await helpers.putEncryptedObject(bkt.name, obj.name, objConf, obj.kmsKey, obj.body);
    const assertion = {
        Bucket: bkt.name,
        Key: obj.name,
        Body: obj.body,
    };
    await assertObjectSSE(assertion, { objConf, obj }, { bktConf, bkt }, {}, testCase);
}

// before CopyObject did not return SSE Key in headers
async function copyObjectAndSSE(
    { copyBkt, objForCopy, copyObj },
    { objConf, obj },
    { bktConf, bkt },
    // migration has its own assert object function
    { index, forceBktSSE, assertObjectSSEFct = assertObjectSSE },
    testCase,
) {
    // variables are defined in before hook, can only be accessed inside test
    const tests = [
        {
            copyArgs: {
                Bucket: copyBkt,
                Key: `${bkt.name}/${objForCopy.name}`,
                CopySource: `${bkt.name}/${objForCopy.name}`,
                // if SSE not provided it uses bucket SSE
            },
            body: objForCopy.body,
        },
        {
            copyArgs: {
                ...helpers.putObjParams(bkt.name, `${obj.name}-copy`, objConf, obj.kmsKey),
                CopySource: `${bkt.name}/${obj.name}`,
            },
            body: obj.body,
        },
        {
            copyArgs: {
                ...helpers.putObjParams(bkt.name, `${obj.name}-copy-from`, objConf, obj.kmsKey),
                CopySource: `${copyBkt}/${copyObj}`,
            },
            body: 'BODY(copy)',
        },
    ];
    const headers = await helpers.s3.copyObject(tests[index].copyArgs);
    let forcedSSE;

    if (forceBktSSE) {
        // copy should migrate bucket key
        const { SSEAlgorithm, KMSMasterKeyID } = await helpers.getBucketSSE(copyBkt);
        assert.strictEqual(headers.ServerSideEncryption, SSEAlgorithm);
        testCase !== 'before' && assert.strictEqual(headers.SSEKMSKeyId, KMSMasterKeyID);
        const keyArn = `${KMSMasterKeyID && isScalityKmsArn(KMSMasterKeyID)
            ? '' : kms.arnPrefix}${KMSMasterKeyID}`;
        const kmsKeyInfo = {
            masterKeyId: getKeyIdFromArn(keyArn),
            masterKeyArn: keyArn,
        };
        forcedSSE = { bktConf: { algo: SSEAlgorithm }, bkt: { kmsKey: KMSMasterKeyID, kmsKeyInfo } };
    }

    const assertion = {
        Bucket: tests[index].copyArgs.Bucket,
        Key: tests[index].copyArgs.Key,
        Body: tests[index].body,
    };
    await assertObjectSSEFct(
        assertion,
        forcedSSE ? { objConf: {}, obj: {} } : { objConf, obj },
        forcedSSE || { bktConf, bkt },
        { headers: testCase === 'before' ? null : headers, put: true },
        testCase,
    );
}

// check MPU headers against the MPU overview MD
// because there is no migration for ongoing MPU
function assertMPUSSEHeaders(actual, expected, algo) {
    if (algo) {
        assert.strictEqual(actual.ServerSideEncryption, algo);
    }
    assert.strictEqual(actual.ServerSideEncryption, expected.ServerSideEncryption);

    if (expected.ServerSideEncryption === 'aws:kms') {
        assert.strictEqual(actual.SSEKMSKeyId, expected.SSEKMSKeyId);
    } else {
        assert.strictEqual(actual.SSEKMSKeyId, undefined);
    }
}

// before has no headers to assert
async function mpuUploadPart({ UploadId, Bucket, Key, Body, PartNumber }, mpuOverviewMDSSE, algo, testCase) {
    const part = await helpers.s3.uploadPart({
        UploadId,
        Bucket,
        Body,
        Key,
        PartNumber,
    });
    testCase !== 'before' && assertMPUSSEHeaders(part, mpuOverviewMDSSE, algo);
    return part;
}

// before has no headers to assert
async function mpuUploadPartCopy(
    { UploadId, Bucket, Key, PartNumber, CopySource, CopySourceRange },
    mpuOverviewMDSSE, algo, testCase
) {
    const part = await helpers.s3.uploadPartCopy({
        UploadId,
        Bucket,
        Key,
        PartNumber,
        CopySource,
        CopySourceRange,
    });
    testCase !== 'before' && assertMPUSSEHeaders(part, mpuOverviewMDSSE, algo);
    return part;
}

// before has no headers to assert
async function mpuComplete({ UploadId, Bucket, Key }, { existingParts, newParts }, mpuOverviewMDSSE, algo, testCase) {
    const extractETag = part => {
        const eTag = part.CopyPartResult?.ETag || part.ETag;
        assert(eTag, `Could not find ETag in part: ${JSON.stringify(part)}`);
        return eTag;
    };
    
    // Build the parts array with proper ETag extraction
    const allParts = [
        ...existingParts.map(part => ({ 
            PartNumber: part.PartNumber, 
            ETag: extractETag(part)
        })),
        ...newParts.map((part, idx) => ({ 
            PartNumber: existingParts.length + idx + 1, 
            ETag: extractETag(part)
        })),
    ];    
    const complete = await helpers.s3.completeMultipartUpload({
        UploadId,
        Bucket,
        Key,
        MultipartUpload: {
             Parts: allParts,
        },
    });
    testCase !== 'before' && assertMPUSSEHeaders(complete, mpuOverviewMDSSE, algo);
    return complete;
}

module.exports = {
    testCases,
    testCasesObj,
    assertObjectSSE,
    deleteBucketSSEBeforeEach,
    tests: {
        getBucketSSEError,
        getBucketNonMandatorySSE,
        getBucketSSE,
        getPreUploadedObject,
        putObjectOverrideSSE,
        copyObjectAndSSE,
        mpuUploadPart,
        mpuUploadPartCopy,
        mpuComplete,
    },
};
