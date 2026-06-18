const assert = require('assert');
const { errors } = require('arsenal');
const {
    S3Client,
    CreateBucketCommand,
    DeleteBucketCommand,
    DeleteBucketCorsCommand,
    PutBucketCorsCommand,
    PutBucketReplicationCommand,
    PutBucketVersioningCommand,
} = require('@aws-sdk/client-s3');
const { series } = require('async');

const getConfig = require('../support/config');
const replicationUtils = require('../../lib/utility/replication');
const BucketUtility = require('../../lib/utility/bucket-util');
const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

const sourceBucket = 'source-bucket';
const destinationBucket = 'destination-bucket';

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(
            err.name,
            expectedErr,
            'incorrect error response ' + `code: should be '${expectedErr}' but got '${err.name}'`,
        );
        assert.strictEqual(
            err.$metadata.httpStatusCode,
            errors[expectedErr].code,
            `incorrect error status code: should be ${errors[expectedErr].code} but got ` +
                `'${err.$metadata.httpStatusCode}'`,
        );
    }
}

// Get parameters for putBucketReplication.
function getReplicationParams(config) {
    return {
        Bucket: sourceBucket,
        ReplicationConfiguration: config,
    };
}

// Get parameters for putBucketVersioning.
function getVersioningParams(status) {
    return {
        Bucket: sourceBucket,
        VersioningConfiguration: {
            Status: status,
        },
    };
}

// Get a complete replication configuration, or remove the specified property.
const replicationConfig = {
    Role: 'arn:aws:iam::account-id:role/src-resource,' + 'arn:aws:iam::account-id:role/dest-resource',
    Rules: [
        {
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'STANDARD',
            },
            Prefix: 'test-prefix',
            Status: 'Enabled',
            ID: 'test-id',
        },
    ],
};

// Set the rules array of a configuration or a property of the first rule.
function setConfigRules(val) {
    const config = Object.assign({}, replicationConfig);
    config.Rules = Array.isArray(val) ? val : [Object.assign({}, config.Rules[0], val)];
    return config;
}

describe('aws-node-sdk test putBucketReplication bucket status', () => {
    let s3;
    let otherAccountS3;
    let replicationAccountS3;
    const replicationParams = getReplicationParams(replicationConfig);

    function checkVersioningError(s3Client, versioningStatus, expectedErr) {
        const versioningParams = getVersioningParams(versioningStatus);
        return series(
            [
                next =>
                    s3Client
                        .send(new PutBucketVersioningCommand(versioningParams))
                        .then(() => next())
                        .catch(next),
                next =>
                    s3Client
                        .send(new PutBucketReplicationCommand(replicationParams))
                        .then(() => next())
                        .catch(next),
            ],
            err => {
                assertError(err, expectedErr);
            },
        );
    }

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        replicationAccountS3 = new BucketUtility('replication', {}).s3;
    });

    it("should return 'NoSuchBucket' error if bucket does not exist", async () => {
        try {
            await s3.send(new PutBucketReplicationCommand(replicationParams));
            throw new Error('Expected NoSuchBucket error');
        } catch (err) {
            if (err.message === 'Expected NoSuchBucket error') {
                throw err;
            }
            assertError(err, 'NoSuchBucket');
        }
    });

    describe('test putBucketReplication bucket versioning status', () => {
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: sourceBucket })));

        afterEach(async () => s3.send(new DeleteBucketCommand({ Bucket: sourceBucket })));

        it('should return AccessDenied if user is not bucket owner', async () => {
            try {
                await otherAccountS3.send(new PutBucketReplicationCommand(replicationParams));
                throw new Error('Expected AccessDenied error');
            } catch (err) {
                if (err.message === 'Expected AccessDenied error') {
                    throw err;
                }
                assert.strictEqual(err.name, 'AccessDenied');
                assert.strictEqual(err.$metadata.httpStatusCode, 403);
            }
        });

        it('should not put configuration on bucket without versioning', async () => {
            try {
                await s3.send(new PutBucketReplicationCommand(replicationParams));
                throw new Error('Expected InvalidRequest error');
            } catch (err) {
                if (err.message === 'Expected InvalidRequest error') {
                    throw err;
                }
                assertError(err, 'InvalidRequest');
            }
        });

        it("should not put configuration on bucket with 'Suspended'" + 'versioning', () =>
            checkVersioningError(s3, 'Suspended', 'InvalidRequest'),
        );

        it('should put configuration on a bucket with versioning', () => checkVersioningError(s3, 'Enabled', null));

        // S3C doesn't support service account. There is no cross account access for replication account.
        // (canonicalId looking like http://acs.zenko.io/accounts/service/replication)
        const itSkipS3C = process.env.S3_END_TO_END ? it.skip : it;
        itSkipS3C('should put configuration on a bucket with versioning if ' + 'user is a replication user', () =>
            checkVersioningError(replicationAccountS3, 'Enabled', null),
        );
    });
});

describe('aws-node-sdk test putBucketReplication configuration rules', () => {
    let s3;

    function checkError(config, expectedErr) {
        const replicationParams = getReplicationParams(config);
        return s3
            .send(new PutBucketReplicationCommand(replicationParams))
            .then(() => {
                if (expectedErr !== null) {
                    return Promise.reject(new Error(`Expected ${expectedErr} error`));
                }
                return Promise.resolve();
            })
            .catch(err => {
                assertError(err, expectedErr);
                return Promise.resolve();
            });
    }

    beforeEach(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: sourceBucket }));
        await s3.send(new PutBucketVersioningCommand(getVersioningParams('Enabled')));
    });

    afterEach(() => s3.send(new DeleteBucketCommand({ Bucket: sourceBucket })));

    replicationUtils.invalidRoleARNs.forEach(ARN => {
        const Role = ARN === '' || ARN === ',' ? ARN : `${ARN},${ARN}`;
        const config = Object.assign({}, replicationConfig, { Role });

        it(
            "should not accept configuration when 'Role' is not a " +
                'comma-separated list of two valid Amazon Resource Names: ' +
                `'${Role}'`,
            () => checkError(config, 'InvalidArgument'),
        );
    });

    it(
        "should not accept configuration when 'Role' is a comma-separated " +
            'list of more than two valid Amazon Resource Names',
        () => {
            const Role =
                'arn:aws:iam::account-id:role/resource-1,' +
                'arn:aws:iam::account-id:role/resource-2,' +
                'arn:aws:iam::account-id:role/resource-3';
            const config = Object.assign({}, replicationConfig, { Role });
            checkError(config, 'InvalidArgument');
        },
    );

    replicationUtils.validRoleARNs.forEach(ARN => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'us-east-2',
            },
        });
        config.Role = ARN;
        const test = `should allow only one role to be specified for external
         locations`;
        itSkipIfE2E(test, () => checkError(config, null));
    });

    it('should allow a combination of storageClasses across rules', () => {
        const config = setConfigRules([
            replicationConfig.Rules[0],
            {
                Destination: {
                    Bucket: `arn:aws:s3:::${destinationBucket}`,
                    StorageClass: 'us-east-2',
                },
                Prefix: 'bar',
                Status: 'Enabled',
            },
        ]);
        config.Role = 'arn:aws:iam::account-id:role/resource,' + 'arn:aws:iam::account-id:role/resource1';
        checkError(config, null);
    });

    itSkipIfE2E(
        'should not allow a comma separated list of roles when' + ' a rule storageClass defines an external location',
        () => {
            const config = {
                Role: 'arn:aws:iam::account-id:role/src-resource,' + 'arn:aws:iam::account-id:role/dest-resource',
                Rules: [
                    {
                        Destination: {
                            Bucket: `arn:aws:s3:::${destinationBucket}`,
                            StorageClass: 'us-east-2',
                        },
                        Prefix: 'test-prefix',
                        Status: 'Enabled',
                    },
                ],
            };
            checkError(config, 'InvalidArgument');
        },
    );

    replicationUtils.validRoleARNs.forEach(ARN => {
        const Role = `${ARN},${ARN}`;
        const config = Object.assign({}, replicationConfig, { Role });

        it(
            "should accept configuration when 'Role' is a comma-separated " +
                `list of two valid Amazon Resource Names: '${Role}'`,
            () => checkError(config, null),
        );
    });

    replicationUtils.invalidBucketARNs.forEach(ARN => {
        const config = setConfigRules({ Destination: { Bucket: ARN } });

        it(
            "should not accept configuration when 'Bucket' is not a " + `valid Amazon Resource Name format: '${ARN}'`,
            () => checkError(config, 'InvalidArgument'),
        );
    });

    it("should not accept configuration when 'Rules' is empty ", () => {
        const config = Object.assign({}, replicationConfig, { Rules: [] });
        return checkError(config, 'MalformedXML');
    });

    it("should not accept configuration when 'Rules' is > 1000", () => {
        const arr = [];
        for (let i = 0; i < 1001; i++) {
            arr.push({
                Destination: { Bucket: destinationBucket },
                Prefix: `${i}-prefix`,
                Status: 'Enabled',
            });
        }
        const config = setConfigRules(arr);
        return checkError(config, 'InvalidRequest');
    });

    it("should not accept configuration when 'ID' length is > 255", () => {
        // Set ID to a string of length 256.
        const config = setConfigRules({ ID: new Array(257).join('x') });
        return checkError(config, 'InvalidArgument');
    });

    it("should not accept configuration when 'ID' is not unique", () => {
        const rule1 = replicationConfig.Rules[0];
        // Prefix is unique, but not the ID.
        const rule2 = Object.assign({}, rule1, { Prefix: 'bar' });
        const config = setConfigRules([rule1, rule2]);
        return checkError(config, 'InvalidRequest');
    });

    it("should accept configuration when 'ID' is not provided for multiple " + 'rules', () => {
        const replicationConfigWithoutID = Object.assign({}, replicationConfig);
        const rule1 = replicationConfigWithoutID.Rules[0];
        delete rule1.ID;
        const rule2 = Object.assign({}, rule1, { Prefix: 'bar' });
        replicationConfigWithoutID.Rules[1] = rule2;
        return checkError(replicationConfigWithoutID, null);
    });

    replicationUtils.validStatuses.forEach(status => {
        const config = setConfigRules({ Status: status });

        it(`should accept configuration when 'Role' is ${status}`, () => checkError(config, null));
    });

    it("should not accept configuration when 'Status' is invalid", () => {
        // Status must either be 'Enabled' or 'Disabled'.
        const config = setConfigRules({ Status: 'Invalid' });
        return checkError(config, 'MalformedXML');
    });

    it("should accept configuration when 'Prefix' is ''", () => {
        const config = setConfigRules({ Prefix: '' });
        return checkError(config, null);
    });

    it("should not accept configuration when 'Prefix' length is > 1024", () => {
        // Set Prefix to a string of length of 1025.
        const config = setConfigRules({
            Prefix: new Array(1026).join('x'),
        });
        return checkError(config, 'InvalidArgument');
    });

    replicationUtils.validStorageClasses.forEach(storageClass => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: storageClass,
            },
        });

        it("should accept configuration when 'StorageClass' is " + `${storageClass}`, () => checkError(config, null));
    });

    // A combination of external destination storage classes.
    replicationUtils.validMultipleStorageClasses.forEach(storageClass => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: storageClass,
            },
        });

        itSkipIfE2E("should accept configuration when 'StorageClass' is " + `${storageClass}`, () =>
            checkError(config, null),
        );
    });

    it("should not accept configuration when 'StorageClass' is invalid", () => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'INVALID',
            },
        });
        return checkError(config, 'MalformedXML');
    });
});

describe('aws-node-sdk test putBucketReplication CORS', () => {
    let s3;
    const bucket = 'source-bucket-cors';

    beforeEach(async () => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3Client(config);
        await s3.send(new CreateBucketCommand({ Bucket: bucket }));
        await s3.send(
            new PutBucketVersioningCommand({
                Bucket: bucket,
                VersioningConfiguration: { Status: 'Enabled' },
            }),
        );
        await s3.send(
            new PutBucketCorsCommand({
                Bucket: bucket,
                CORSConfiguration: {
                    CORSRules: [
                        {
                            AllowedOrigins: ['*'],
                            AllowedMethods: ['PUT'],
                            AllowedHeaders: ['*'],
                        },
                    ],
                },
            }),
        );
    });

    afterEach(async () => {
        try {
            await s3.send(new DeleteBucketCorsCommand({ Bucket: bucket }));
        } catch (err) {
            if (err.name !== 'NoSuchCORSConfiguration') {
                throw err;
            }
        }
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    });

    it('should return malformed XML error in XML is invalid', async () => {
        const replicationParams = {
            Bucket: bucket,
            ReplicationConfiguration: {
                Role: 'arn:aws:iam::account-id:role/src-resource,' + 'arn:aws:iam::account-id:role/dest-resource',
                Rules: [],
            },
        };
        const command = new PutBucketReplicationCommand(replicationParams);
        command.middlewareStack.add(
            next => async args => {
                if (args.request && args.request.headers) {
                    // eslint-disable-next-line no-param-reassign
                    args.request.headers.Origin = 'http://example.com';
                }
                return next(args);
            },
            {
                name: 'injectOriginHeader',
                step: 'build',
                priority: 'high',
            },
        );

        try {
            await s3.send(command);
            assert.fail('Expected MalformedXML error');
        } catch (err) {
            assertError(err, 'MalformedXML');
        }
    });
});
