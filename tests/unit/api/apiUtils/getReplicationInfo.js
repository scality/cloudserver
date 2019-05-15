const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const AuthInfo = require('arsenal').auth.AuthInfo;
const getReplicationInfo =
      require('../../../../lib/api/apiUtils/object/getReplicationInfo');

function _getObjectReplicationInfo(replicationConfig) {
    const bucketInfo = new BucketInfo(
        'testbucket', 'someCanonicalId', 'accountDisplayName',
        new Date().toJSON(),
        null, null, null, null, null, null, null, null, null,
        replicationConfig);
    return getReplicationInfo('fookey', bucketInfo, true, 123, null, null);
}

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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
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
            isNFS: null,
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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
        assert.deepStrictEqual(replicationInfo, undefined);
    });

    it('should get replication info with single cloud target', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
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
            isNFS: null,
        });
    });

    it('should get replication info with multiple cloud targets', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend,azurebackend',
            }],
            destination: 'tosomewhere',
        };
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }, {
                site: 'azurebackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend,azurebackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3,azure',
            isNFS: null,
        });
    });

    it('should get replication info with multiple cloud targets and ' +
    'preferred read location', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend:preferred_read,azurebackend',
            }],
            destination: 'tosomewhere',
            preferredReadLocation: 'awsbackend',
        };
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
        assert.deepStrictEqual(replicationInfo, {
            status: 'PENDING',
            backends: [{
                site: 'awsbackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }, {
                site: 'azurebackend',
                status: 'PENDING',
                dataStoreVersionId: '',
            }],
            content: ['METADATA'],
            destination: 'tosomewhere',
            storageClass: 'awsbackend:preferred_read,azurebackend',
            role: 'arn:aws:iam::root:role/s3-replication-role',
            storageType: 'aws_s3,azure',
            isNFS: null,
        });
    });

    it('should not get replication info when service account type ' +
    'cannot trigger replication', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const bucketInfo = new BucketInfo(
            'testbucket', 'abcdef/lifecycle', 'Lifecycle Service Account',
            new Date().toJSON(),
            null, null, null, null, null, null, null, null, null,
            replicationConfig);
        const authInfo = new AuthInfo({
            canonicalID: 'abcdef/lifecycle',
            accountDisplayName: 'Lifecycle Service Account',
        });
        const replicationInfo = getReplicationInfo(
            'fookey', bucketInfo, true, 123, null, null, authInfo);
        assert.deepStrictEqual(replicationInfo, undefined);
    });

    it('should get replication info when service account type can ' +
    'trigger replication', () => {
        const replicationConfig = {
            role: 'arn:aws:iam::root:role/s3-replication-role',
            rules: [{
                prefix: '',
                enabled: true,
                storageClass: 'awsbackend',
            }],
            destination: 'tosomewhere',
        };
        const bucketInfo = new BucketInfo(
            'testbucket', 'abcdef/md-ingestion',
            'Metadata Ingestion Service Account',
            new Date().toJSON(),
            null, null, null, null, null, null, null, null, null,
            replicationConfig);
        const authInfo = new AuthInfo({
            canonicalID: 'abcdef/md-ingestion',
            accountDisplayName: 'Metadata Ingestion Service Account',
        });
        const replicationInfo = getReplicationInfo(
            'fookey', bucketInfo, true, 123, null, null, authInfo);
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
            isNFS: null,
        });
    });
});
