const replicationUtils = {
    requiredConfigProperties: [
        'Role',
        'Rules',
        'Status',
        'Prefix',
        'Destination',
        'Bucket',
    ],
    optionalConfigProperties: [
        'ID',
        'StorageClass',
    ],
    invalidRoleARNs: [
        'arn:partition:service::account-id:resourcetype', // Missing resource.
        'arn:partition:service::account-id:resourcetype/resource/extra-value',
        'arn:partition:service::account-id:resourcetype:resource:extra-value',
    ],
    // Role value should be an Amazon Resource Name IAM user name format.
    validRoleARNs: [
        'arn:partition:service::account-id:resourcetype/resource',
        'arn:partition:service::account-id:resourcetype:resource',
    ],
    invalidBucketARNs: [
        'arn:partition:service::resource', // Missing an omitted account-id.
        'arn:partition:service::::resource',
    ],
    validStatuses: [
        'Enabled',
        'Disabled',
    ],
    validStorageClasses: [
        'STANDARD',
        'STANDARD_IA',
        'REDUCED_REDUNDANCY',
    ],
};

module.exports = replicationUtils;
