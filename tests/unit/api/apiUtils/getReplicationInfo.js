const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const getReplicationInfo =
      require('../../../../lib/api/apiUtils/object/getReplicationInfo');
const { makeAuthInfo } = require('../../helpers');

function _getObjectReplicationInfo(s3config, replicationConfig, authInfo, isDeleteMarker) {
    const bucketInfo = new BucketInfo(
        'testbucket', 'someCanonicalId', 'accountDisplayName',
        new Date().toJSON(),
        null, null, null, null, null, null, null, null, null,
        replicationConfig);
    return getReplicationInfo(s3config,
         'fookey', bucketInfo, true, 123, null, null, authInfo, isDeleteMarker);
}

const TEST_CONFIG = {
    locationConstraints: {
        awsbackend: {
            type: 'aws_s3',
            legacyAwsBehavior: true,
            details: {
                awsEndpoint: 's3.amazonaws.com',
                bucketName: 'awsbucket',
                bucketMatch: true,
                credentialsProfile: 'default',
            },
        },
    },
    replicationEndpoints: [{
        site: 'zenko',
        servers: ['127.0.0.1:8000'],
        default: true,
    }, {
        site: 'us-east-2',
        type: 'aws_s3',
    }],
};

describe('getReplicationInfo helper', () => {
    it('should get replication info when rules are enabled', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3',
        });
    });

    it('should get replication info when action comming from a non-lifecycle session', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };

        const authInfo = makeAuthInfo('accessKey1', null, 'another-session');
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG,
            replicationConfig, authInfo, true);

        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3',
        });
    });

    it('should get replication info when action comming from a lifecycle session ' +
    'but action is not delete marker', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };

        const authInfo = makeAuthInfo('accessKey1', null, 'backbeat-lifecycle');
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG,
            replicationConfig, authInfo, false);

        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3',
        });
    });

    it('should not get replication info when rules are disabled', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: false,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
        assert.deepStrictEqual(replicationInfo, undefined);
    });

    it('should not get replication info when action coming from lifecycle session', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };

        const authInfo = makeAuthInfo('accessKey1', null, 'backbeat-lifecycle');
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG,
            replicationConfig, authInfo, true);

        assert.deepStrictEqual(replicationInfo, undefined);
    });

    it('should get replication info with default StorageClass when rules are enabled', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role-1,arn:aws:iam::root:role/s3-replication-role-2',
            rules: [{
                prefix: '',
                enabled: true,
            }],
            destination: 'tosomewhere',
        };
        const replicationInfo = _getObjectReplicationInfo(TEST_CONFIG, replicationConfig);
        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'zenko',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'zenko',
            role: 'arn:aws:iam::root:role/s3-replication-role-1,arn:aws:iam::root:role/s3-replication-role-2',
            storageType: '',
        });
    });

    it('should return undefined with specified StorageClass mode if no replication endpoint is configured', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const configWithNoReplicationEndpoint = {
            locationConstraints: TEST_CONFIG.locationConstraints,
            replicationEndpoints: [],
        };
        const replicationInfo = _getObjectReplicationInfo(configWithNoReplicationEndpoint,
            replicationConfig);
        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3',
        });
    });

    it('should return undefined with default StorageClass if no replication endpoint is configured', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role-1,arn:aws:iam::root:role/s3-replication-role-2',
            rules: [{
                prefix: '',
                enabled: true,
            }],
            destination: 'tosomewhere',
        };
        const configWithNoReplicationEndpoint = {
            locationConstraints: TEST_CONFIG.locationConstraints,
            replicationEndpoints: [],
        };
        const replicationInfo = _getObjectReplicationInfo(configWithNoReplicationEndpoint,
            replicationConfig);
        assert.deepStrictEqual(replicationInfo, undefined);
    });
});
