const assert = require('assert');
const kms = require('../../../lib/kms/wrapper');
const filekms = require('../../../lib/kms/file/backend');
const { DummyRequestLogger } = require('../../unit/helpers');
const helpers = require('./helpers');

const log = new DummyRequestLogger();
const arnPrefixReg = new RegExp(`^${kms.arnPrefix}`);

function createInternalKeyArn() {
    return new Promise((resolve, reject) =>
        filekms.createBucketKey('bucket', log, (err, keyId, keyArn) => (err ? reject(err) : resolve(keyArn))),
    );
}

async function getBucketSSEMD(Bucket) {
    const bucketMD = await helpers.MD.getBucket(Bucket, log);
    return bucketMD.getServerSideEncryption();
}

describe('SSE KMS migration of internal bucket keys with ARN prefix', () => {
    before(async () => helpers.MD.setup());

    [
        {
            name: 'created by S3C',
            Bucket: 'enc-bkt-internal-arn-master-key',
            keyField: 'masterKeyId',
            setBucketKey: async (Bucket, keyArn) => {
                const bucketMD = await helpers.MD.getBucket(Bucket, log);
                bucketMD.setServerSideEncryption({
                    cryptoScheme: 1,
                    algorithm: 'aws:kms',
                    mandatory: true,
                    masterKeyId: keyArn,
                });
                await helpers.MD.updateBucket(Bucket, bucketMD, log);
            },
        },
        {
            name: 'set by the user',
            Bucket: 'enc-bkt-internal-arn-configured-key',
            keyField: 'configuredMasterKeyId',
            setBucketKey: (Bucket, keyArn) =>
                helpers.s3.putBucketEncryption({
                    Bucket,
                    ServerSideEncryptionConfiguration: helpers.hydrateSSEConfig({
                        algo: 'aws:kms',
                        masterKeyId: keyArn,
                    }),
                }),
        },
    ].forEach(({ name, Bucket, keyField, setBucketKey }) =>
        describe(`bucket key ${name}`, () => {
            const oldObj = { Key: 'old-obj', Body: 'BODY(old-obj)' };
            const newObj = { Key: 'new-obj', Body: 'BODY(new-obj)' };
            let internalKeyArn;
            let replacedSSE;

            before(async () => {
                internalKeyArn = await createInternalKeyArn();
                await helpers.s3.createBucket({ Bucket });
                // Put before the bucket has a key: an object request would replace it
                await helpers.putEncryptedObject(
                    Bucket,
                    oldObj.Key,
                    { algo: 'aws:kms', masterKeyId: true },
                    internalKeyArn,
                    oldObj.Body,
                );
                await setBucketKey(Bucket, internalKeyArn);
                const sse = await getBucketSSEMD(Bucket);
                assert.strictEqual(sse[keyField], internalKeyArn);
            });

            after(async () => helpers.cleanup(Bucket));

            it('should replace the bucket key by a key of the default KMS on HeadObject', async () => {
                await helpers.s3.headObject({ Bucket, Key: oldObj.Key });
                replacedSSE = await getBucketSSEMD(Bucket);
                assert.match(replacedSSE[keyField], arnPrefixReg);
            });

            it('should keep the internal key of an existing object and decrypt it', async () => {
                const sseMD = await helpers.getObjectMDSSE(Bucket, oldObj.Key);
                assert.strictEqual(sseMD.SSEKMSKeyId, internalKeyArn);
                const get = await helpers.s3.getObject({ Bucket, Key: oldObj.Key });
                assert.strictEqual(get.Body, oldObj.Body);
            });

            it('should encrypt a new object with the replacing key', async () => {
                await helpers.s3.putObject({ Bucket, ...newObj });
                const sseMD = await helpers.getObjectMDSSE(Bucket, newObj.Key);
                assert.match(sseMD.SSEKMSKeyId, arnPrefixReg);
                assert.strictEqual(sseMD.SSEKMSKeyId, replacedSSE[keyField]);
                const get = await helpers.s3.getObject({ Bucket, Key: newObj.Key });
                assert.strictEqual(get.Body, newObj.Body);
            });

            it('should keep the replacing key on later requests', async () => {
                await helpers.s3.headObject({ Bucket, Key: oldObj.Key });
                assert.deepStrictEqual(await getBucketSSEMD(Bucket), replacedSSE);
            });
        }),
    );
});
