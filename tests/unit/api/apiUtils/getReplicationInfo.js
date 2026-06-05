const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const AuthInfo = require('arsenal').auth.AuthInfo;
const getReplicationInfo = require('../../../../lib/api/apiUtils/object/getReplicationInfo');

function _getObjectReplicationInfo(s3config, replicationConfig, key, objectMD) {
    const bucketInfo = new BucketInfo(
        'testbucket',
        'someCanonicalId',
        'accountDisplayName',
        new Date().toJSON(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        replicationConfig,
    );
    return getReplicationInfo(s3config, key || 'fookey', bucketInfo, true, 123, null, objectMD || null);
}

const TEST_CONFIG = {
    locationConstraints: {
        awsbackend: {
            type: 'aws_s3',
            objectId: 'awsbackend',
            legacyAwsBehavior: true,
            details: {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 'awsbucket',
                bucketMatch: true,
                credentialsProfile: 'default',
            },
        },
        azurebackend: {
            type: 'azure',
            objectId: 'azurebackend',
            legacyAwsBehavior: true,
            details: {
                azureStorageEndpoint: 'https://fakeaccountname.blob.core.fake.net/',
                azureStorageAccountName: 'fakeaccountname',
                azureStorageAccessKey: 'Fake00Key001',
                bucketMatch: true,
                azureContainerName: 's3test',
            },
        },
        'crr-site': {
            objectId: 'crr-site',
        },
    },
    replicationEndpoints: [
        {
            site: 'zenko',
            servers: ['127.0.0.1:8000'],
            default: true,
        },
        {
            site: 'us-east-2',
            type: 'aws_s3',
        },
    ],
};

const TWO_PART_ROLE = 'arn:aws:iam::root:role/src-role,arn:aws:iam::root:role/dst-role';

describe('getReplicationInfo helper', () => {
    describe('V1 format (single rule match)', () => {
        it('should get replication info when rules are enabled', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'awsbackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should not get replication info when rules are disabled', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: false,
                        storageClass: 'awsbackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.deepStrictEqual(replicationInfo, undefined);
        });

        it('should match all V1 rules with overlapping prefixes', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend',
                    },
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'azurebackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.strictEqual(replicationInfo.backends.length, 2);
            assert.deepStrictEqual(replicationInfo.backends.map(b => b.site).sort(), ['awsbackend', 'azurebackend']);
        });

        it('should get replication info with multiple cloud targets (legacy comma-separated)', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend,azurebackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'awsbackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                    {
                        site: 'azurebackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should get replication info with multiple cloud targets and ' + 'preferred read location', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend:preferred_read,azurebackend',
                    },
                ],
                destination: 'tosomewhere',
                preferredReadLocation: 'awsbackend',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'awsbackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                    {
                        site: 'azurebackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should not get replication info when service account type ' + 'cannot trigger replication', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const bucketInfo = new BucketInfo(
                'testbucket',
                'abcdef/lifecycle',
                'Lifecycle Service Account',
                new Date().toJSON(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                replicationConfig,
            );
            const authInfo = new AuthInfo({
                canonicalID: 'abcdef/lifecycle',
                accountDisplayName: 'Lifecycle Service Account',
            });
            const replicationInfo = getReplicationInfo(
                TEST_CONFIG,
                'fookey',
                bucketInfo,
                true,
                123,
                null,
                null,
                authInfo,
            );
            assert.deepStrictEqual(replicationInfo, undefined);
        });

        it('should get replication info when service account type can ' + 'trigger replication', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const bucketInfo = new BucketInfo(
                'testbucket',
                'abcdef/md-ingestion',
                'Metadata Ingestion Service Account',
                new Date().toJSON(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                replicationConfig,
            );
            const authInfo = new AuthInfo({
                canonicalID: 'abcdef/md-ingestion',
                accountDisplayName: 'Metadata Ingestion Service Account',
            });
            const replicationInfo = getReplicationInfo(
                TEST_CONFIG,
                'fookey',
                bucketInfo,
                true,
                123,
                null,
                null,
                authInfo,
            );
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'awsbackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should fall back to default StorageClass and resolve as a CRR backend', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                    },
                ],
                destination: 'tosomewhere',
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'zenko',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                        destination: 'tosomewhere',
                        role: 'arn:aws:iam::root:role/dst-role',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should return replication info with cloud backend even when no replication endpoint is configured', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        storageClass: 'awsbackend',
                    },
                ],
                destination: 'tosomewhere',
            };
            const configWithNoReplicationEndpoint = {
                locationConstraints: TEST_CONFIG.locationConstraints,
                replicationEndpoints: [],
            };
            const replicationInfo = _getObjectReplicationInfo(configWithNoReplicationEndpoint, replicationConfig);
            assert.deepStrictEqual(replicationInfo, {
                status: 'PENDING',
                backends: [
                    {
                        site: 'awsbackend',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                    },
                ],
                content: ['METADATA'],
                role: 'arn:aws:iam::root:role/src-role',
                isNFS: undefined,
            });
        });

        it('should return undefined with default StorageClass if no replication endpoint is configured', () => {
            const replicationConfig = {
                role: TWO_PART_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                    },
                ],
                destination: 'tosomewhere',
            };
            const configWithNoReplicationEndpoint = {
                locationConstraints: TEST_CONFIG.locationConstraints,
                replicationEndpoints: [],
            };
            const replicationInfo = _getObjectReplicationInfo(configWithNoReplicationEndpoint, replicationConfig);
            assert.deepStrictEqual(replicationInfo, undefined);
        });
    });

    // --- V2 Format Tests (multi-rule matching) ---
    describe('V2 format (multi-rule matching)', () => {
        const V2_ROLE = 'arn:aws:iam::123456:role/src-role,arn:aws:iam::111111:role/dst-role';

        it('should match all rules with overlapping prefixes', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                    {
                        prefix: 'docs',
                        enabled: true,
                        priority: 2,
                        storageClass: 'azurebackend',
                        destination: 'arn:aws:s3:::bucket-b',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo.status, 'PENDING');
            assert.strictEqual(replicationInfo.backends.length, 2);
            const awsBackend = replicationInfo.backends.find(b => b.site === 'awsbackend');
            const azureBackend = replicationInfo.backends.find(b => b.site === 'azurebackend');
            assert.ok(awsBackend);
            assert.ok(azureBackend);
            // Cloud backends do not carry per-backend destination/role
            // or storageType (location type is resolved from config).
            assert.strictEqual(awsBackend.destination, undefined);
            assert.strictEqual(awsBackend.role, undefined);
            assert.strictEqual(awsBackend.storageType, undefined);
            assert.strictEqual(azureBackend.destination, undefined);
            assert.strictEqual(azureBackend.role, undefined);
            assert.strictEqual(azureBackend.storageType, undefined);
        });

        it('should only match rules whose prefix matches the object key', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                    {
                        prefix: 'logs',
                        enabled: true,
                        priority: 2,
                        storageClass: 'azurebackend',
                        destination: 'arn:aws:s3:::bucket-b',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo.backends.length, 1);
            assert.strictEqual(replicationInfo.backends[0].site, 'awsbackend');
        });

        it('should keep two CRR backends for same site but different destinations', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'crr-site',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                    {
                        prefix: 'docs',
                        enabled: true,
                        priority: 5,
                        storageClass: 'crr-site',
                        destination: 'arn:aws:s3:::bucket-b',
                        account: '222222',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo.backends.length, 2);
            const buckets = replicationInfo.backends.map(b => b.destination).sort();
            assert.deepStrictEqual(buckets, ['arn:aws:s3:::bucket-a', 'arn:aws:s3:::bucket-b']);
        });

        it('should dedup CRR rules with same (site, destination, role)', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'crr-site',
                        destination: 'arn:aws:s3:::bucket-a',
                        account: '222222',
                    },
                    {
                        prefix: 'docs',
                        enabled: true,
                        priority: 5,
                        storageClass: 'crr-site',
                        destination: 'arn:aws:s3:::bucket-a',
                        account: '222222',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo.backends.length, 1);
            assert.strictEqual(replicationInfo.backends[0].destination, 'arn:aws:s3:::bucket-a');
            assert.strictEqual(replicationInfo.backends[0].role, 'arn:aws:iam::222222:role/dst-role');
        });

        it('should skip disabled rules', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                    {
                        prefix: '',
                        enabled: false,
                        priority: 2,
                        storageClass: 'azurebackend',
                        destination: 'arn:aws:s3:::bucket-b',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo.backends.length, 1);
            assert.strictEqual(replicationInfo.backends[0].site, 'awsbackend');
        });

        it('should return undefined when no V2 rules match', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: 'logs/',
                        enabled: true,
                        priority: 1,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'docs/report.pdf');
            assert.strictEqual(replicationInfo, undefined);
        });

        it('should set top-level role to the source role only', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-a',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'fookey');
            assert.strictEqual(replicationInfo.role, 'arn:aws:iam::123456:role/src-role');
        });

        it('should handle mixed CRR and cloud backends', () => {
            const replicationConfig = {
                role: V2_ROLE,
                rules: [
                    {
                        prefix: '',
                        enabled: true,
                        priority: 1,
                        storageClass: 'crr-site',
                        destination: 'arn:aws:s3:::bucket-a',
                        account: '222222',
                    },
                    {
                        prefix: '',
                        enabled: true,
                        priority: 2,
                        storageClass: 'awsbackend',
                        destination: 'arn:aws:s3:::bucket-b',
                    },
                ],
            };
            const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig, 'fookey');
            assert.strictEqual(replicationInfo.backends.length, 2);
            const crrBackend = replicationInfo.backends.find(b => b.site === 'crr-site');
            const cloudBackend = replicationInfo.backends.find(b => b.site === 'awsbackend');
            assert.ok(crrBackend);
            assert.ok(cloudBackend);
            // CRR backend has destination/role
            assert.strictEqual(crrBackend.destination, 'arn:aws:s3:::bucket-a');
            assert.strictEqual(crrBackend.role, 'arn:aws:iam::222222:role/dst-role');
            assert.strictEqual(crrBackend.storageType, undefined);
            // Cloud backend has neither (location type is resolved from config)
            assert.strictEqual(cloudBackend.destination, undefined);
            assert.strictEqual(cloudBackend.role, undefined);
            assert.strictEqual(cloudBackend.storageType, undefined);
        });
    });
});
