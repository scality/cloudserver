const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const getReplicationInfo =
      require('../../../../lib/api/apiUtils/object/getReplicationInfo');
const { makeAuthInfo } = require('../../helpers');

function _getObjectReplicationInfo(replicationConfig, authInfo, isDeleteMarker) {
    const bucketInfo = new BucketInfo(
        'testbucket', 'someCanonicalId', 'accountDisplayName',
        new Date().toJSON(),
        null, null, null, null, null, null, null, null, null,
        replicationConfig);
    return getReplicationInfo('fookey', bucketInfo, true, 123, null, null, authInfo, isDeleteMarker);
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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig, authInfo, true);

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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig, authInfo, false);

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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig);
        assert.deepStrictEqual(replicationInfo, undefined);
    });

    it('should not get replication info when action comming from lifecycle session', () => {
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
        const replicationInfo = _getObjectReplicationInfo(replicationConfig, authInfo, true);

        assert.deepStrictEqual(replicationInfo, undefined);
    });
});
