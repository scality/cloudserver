const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');
const { series } = require('async');

const getConfig = require('../support/config');
const replicationUtils = require('../../lib/utility/replication');
const BucketUtility = require('../../lib/utility/bucket-util');

const sourceBucket = 'source-bucket';
const destinationBucket = 'destination-bucket';

// Check for the expected error response code and status code.
function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be 400 but got ' +
            `'${err.statusCode}'`);
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
    Role: 'arn:aws:iam::account-id:role/src-resource,' +
        'arn:aws:iam::account-id:role/dest-resource',
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
    config.Rules = Array.isArray(val) ? val :
        [Object.assign({}, config.Rules[0], val)];
    return config;
}

describe('aws-node-sdk test putBucketReplication bucket status', () => {
    let s3;
    let otherAccountS3;
    const replicationParams = getReplicationParams(replicationConfig);

    function checkVersioningError(versioningStatus, expectedErr, cb) {
        const versioningParams = getVersioningParams(versioningStatus);
        return series([
            next => s3.putBucketVersioning(versioningParams, next),
            next => s3.putBucketReplication(replicationParams, next),
        ], err => {
            assertError(err, expectedErr);
            return cb();
        });
    }

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        otherAccountS3 = new BucketUtility('lisa', {}).s3;
        return done();
    });

    it('should return \'NoSuchBucket\' error if bucket does not exist', done =>
        s3.putBucketReplication(replicationParams, err => {
            assertError(err, 'NoSuchBucket');
            return done();
        }));

    describe('test putBucketReplication bucket versioning status', () => {
        beforeEach(done => s3.createBucket({ Bucket: sourceBucket }, done));

        afterEach(done => s3.deleteBucket({ Bucket: sourceBucket }, done));

        it('should return AccessDenied if user is not bucket owner', done =>
            otherAccountS3.putBucketReplication(replicationParams,
            err => {
                assert(err);
                assert.strictEqual(err.code, 'AccessDenied');
                assert.strictEqual(err.statusCode, 403);
                return done();
            }));

        it('should not put configuration on bucket without versioning', done =>
            s3.putBucketReplication(replicationParams, err => {
                assertError(err, 'InvalidRequest');
                return done();
            }));

        it('should not put configuration on bucket with \'Suspended\'' +
            'versioning', done =>
            checkVersioningError('Suspended', 'InvalidRequest', done));

        it('should put configuration on a bucket with versioning', done =>
            checkVersioningError('Enabled', null, done));
    });
});

describe('aws-node-sdk test putBucketReplication configuration rules', () => {
    let s3;

    function checkError(config, expectedErr, cb) {
        const replicationParams = getReplicationParams(config);
        s3.putBucketReplication(replicationParams, err => {
            assertError(err, expectedErr);
            return cb();
        });
    }

    beforeEach(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        return series([
            next => s3.createBucket({ Bucket: sourceBucket }, next),
            next =>
                s3.putBucketVersioning(getVersioningParams('Enabled'), next),
        ], err => done(err));
    });

    afterEach(done => s3.deleteBucket({ Bucket: sourceBucket }, done));

    replicationUtils.invalidRoleARNs.forEach(ARN => {
        const Role = ARN === '' || ARN === ',' ? ARN : `${ARN},${ARN}`;
        const config = Object.assign({}, replicationConfig, { Role });

        it('should not accept configuration when \'Role\' is not a ' +
            'comma-separated list of two valid Amazon Resource Names: ' +
            `'${Role}'`, done =>
            checkError(config, 'InvalidArgument', done));
    });

    it('should not accept configuration when \'Role\' is a comma-separated ' +
        'list of more than two valid Amazon Resource Names',
        done => {
            const Role = 'arn:aws:iam::account-id:role/resource-1,' +
                'arn:aws:iam::account-id:role/resource-2,' +
                'arn:aws:iam::account-id:role/resource-3';
            const config = Object.assign({}, replicationConfig, { Role });
            checkError(config, 'InvalidArgument', done);
        });

    replicationUtils.validRoleARNs.forEach(ARN => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'us-east-1',
            },
        });
        config.Role = ARN;

        it('should accept configuration if \'Role\' is a single valid ' +
            `Amazon Resource Name: '${ARN}', and a rule storageClass defines ` +
            'an external location', done =>
            checkError(config, null, done));
    });

    it('should not allow a combination of storageClasses when one ' +
        'defines an external location', done => {
        const config = setConfigRules([replicationConfig.Rules[0], {
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: 'us-east-1',
            },
            Prefix: 'bar',
            Status: 'Enabled',
        }]);
        config.Role = 'arn:aws:iam::account-id:role/resource';
        checkError(config, 'InvalidRequest', done);
    });

    it('should not allow a comma separated list of roles when a rule ' +
        'storageClass defines an external location', done => {
        const config = {
            Role: 'arn:aws:iam::account-id:role/src-resource,' +
                'arn:aws:iam::account-id:role/dest-resource',
            Rules: [
                {
                    Destination: {
                        Bucket: `arn:aws:s3:::${destinationBucket}`,
                        StorageClass: 'us-east-1',
                    },
                    Prefix: 'test-prefix',
                    Status: 'Enabled',
                },
            ],
        };
        checkError(config, 'InvalidArgument', done);
    });

    replicationUtils.validRoleARNs.forEach(ARN => {
        const Role = `${ARN},${ARN}`;
        const config = Object.assign({}, replicationConfig, { Role });

        it('should accept configuration when \'Role\' is a comma-separated ' +
            `list of two valid Amazon Resource Names: '${Role}'`, done =>
            checkError(config, null, done));
    });

    replicationUtils.invalidBucketARNs.forEach(ARN => {
        const config = setConfigRules({ Destination: { Bucket: ARN } });

        it('should not accept configuration when \'Bucket\' is not a ' +
            `valid Amazon Resource Name format: '${ARN}'`, done =>
            checkError(config, 'InvalidArgument', done));
    });

    it('should not accept configuration when \'Rules\' is empty ', done => {
        const config = Object.assign({}, replicationConfig, { Rules: [] });
        return checkError(config, 'MalformedXML', done);
    });

    it('should not accept configuration when \'Rules\' is > 1000', done => {
        const arr = [];
        for (let i = 0; i < 1001; i++) {
            arr.push({
                Destination: { Bucket: destinationBucket },
                Prefix: `${i}-prefix`,
                Status: 'Enabled',
            });
        }
        const config = setConfigRules(arr);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should not accept configuration when \'ID\' length is > 255', done => {
        // Set ID to a string of length 256.
        const config = setConfigRules({ ID: new Array(257).join('x') });
        return checkError(config, 'InvalidArgument', done);
    });

    it('should not accept configuration when \'ID\' is not unique', done => {
        const rule1 = replicationConfig.Rules[0];
        // Prefix is unique, but not the ID.
        const rule2 = Object.assign({}, rule1, { Prefix: 'bar' });
        const config = setConfigRules([rule1, rule2]);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should accept configuration when \'ID\' is not provided for multiple ' +
        'rules', done => {
        const replicationConfigWithoutID = Object.assign({}, replicationConfig);
        const rule1 = replicationConfigWithoutID.Rules[0];
        delete rule1.ID;
        const rule2 = Object.assign({}, rule1, { Prefix: 'bar' });
        replicationConfigWithoutID.Rules[1] = rule2;
        return checkError(replicationConfigWithoutID, null, done);
    });

    replicationUtils.validStatuses.forEach(status => {
        const config = setConfigRules({ Status: status });

        it(`should accept configuration when 'Role' is ${status}`, done =>
            checkError(config, null, done));
    });

    it('should not accept configuration when \'Status\' is invalid', done => {
        // Status must either be 'Enabled' or 'Disabled'.
        const config = setConfigRules({ Status: 'Invalid' });
        return checkError(config, 'MalformedXML', done);
    });

    it('should accept configuration when \'Prefix\' is \'\'',
        done => {
            const config = setConfigRules({ Prefix: '' });
            return checkError(config, null, done);
        });

    it('should not accept configuration when \'Prefix\' length is > 1024',
        done => {
            // Set Prefix to a string of length of 1025.
            const config = setConfigRules({
                Prefix: new Array(1026).join('x'),
            });
            return checkError(config, 'InvalidArgument', done);
        });

    it('should not accept configuration when rules contain overlapping ' +
        '\'Prefix\' values: new prefix starts with used prefix', done => {
        const config = setConfigRules([replicationConfig.Rules[0], {
            Destination: { Bucket: `arn:aws:s3:::${destinationBucket}` },
            Prefix: 'test-prefix/more-content',
            Status: 'Enabled',
        }]);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should not accept configuration when rules contain overlapping ' +
        '\'Prefix\' values: used prefix starts with new prefix', done => {
        const config = setConfigRules([replicationConfig.Rules[0], {
            Destination: { Bucket: `arn:aws:s3:::${destinationBucket}` },
            Prefix: 'test',
            Status: 'Enabled',
        }]);
        return checkError(config, 'InvalidRequest', done);
    });

    it('should not accept configuration when \'Destination\' properties of ' +
        'two or more rules specify different buckets', done => {
        const config = setConfigRules([replicationConfig.Rules[0], {
            Destination: { Bucket: `arn:aws:s3:::${destinationBucket}-1` },
            Prefix: 'bar',
            Status: 'Enabled',
        }]);
        return checkError(config, 'InvalidRequest', done);
    });

    replicationUtils.validStorageClasses.forEach(storageClass => {
        const config = setConfigRules({
            Destination: {
                Bucket: `arn:aws:s3:::${destinationBucket}`,
                StorageClass: storageClass,
            },
        });

        it('should accept configuration when \'StorageClass\' is ' +
            `${storageClass}`, done => checkError(config, null, done));
    });

    it('should not accept configuration when \'StorageClass\' is invalid',
        done => {
            const config = setConfigRules({
                Destination: {
                    Bucket: `arn:aws:s3:::${destinationBucket}`,
                    StorageClass: 'INVALID',
                },
            });
            return checkError(config, 'MalformedXML', done);
        });
});
