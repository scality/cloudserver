const assert = require('assert');
const { S3 } = require('aws-sdk');

const getConfig = require('../support/config');

const bucket = `versioning-bucket-${Date.now()}`;
const config = getConfig('default', { signatureVersion: 'v4' });
const configReplication = getConfig('replication',
    { signatureVersion: 'v4' });
const s3 = new S3(config);
describe('aws-node-sdk test bucket versioning', () => {
    this.timeout(60000);
    let replicationAccountS3;

    // setup test
    beforeAll(done => {
        replicationAccountS3 = new S3(configReplication);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    afterAll(done => s3.deleteBucket({ Bucket: bucket }, done));

    test('should not accept empty versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {},
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                expect(error.statusCode).toBe(400);
                expect(error.code).toBe('IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    test('should retrieve an empty versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            expect(error).toBe(null);
            assert.deepStrictEqual(data, {});
            done();
        });
    });

    test('should not accept versioning configuration w/o "Status"', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                expect(error.statusCode).toBe(400);
                expect(error.code).toBe('IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    test('should retrieve an empty versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            expect(error).toBe(null);
            assert.deepStrictEqual(data, {});
            done();
        });
    });

    test('should not accept versioning configuration w/ invalid value', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'fun',
                Status: 'let\'s do it',
            },
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                expect(error.statusCode).toBe(400);
                expect(error.code).toBe('IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    test('should not accept versioning with MFA Delete enabled', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, error => {
            expect(error).not.toEqual(null);
            expect(error.statusCode).toBe(501);
            expect(error.code).toBe('NotImplemented');
            done();
        });
    });

    test('should accept versioning with MFA Delete disabled', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Disabled',
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, error => {
            expect(error).toEqual(null);
            done();
        });
    });

    test('should retrieve the valid versioning configuration', done => {
        const params = { Bucket: bucket };
        // s3.getBucketVersioning(params, done);
        s3.getBucketVersioning(params, (error, data) => {
            expect(error).toBe(null);
            assert.deepStrictEqual(data, { MFADelete: 'Disabled',
                Status: 'Enabled' });
            done();
        });
    });

    test('should accept valid versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, done);
    });

    test('should accept valid versioning configuration if user is a ' +
    'replication user', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        replicationAccountS3.putBucketVersioning(params, done);
    });

    test('should retrieve the valid versioning configuration', done => {
        const params = { Bucket: bucket };
        // s3.getBucketVersioning(params, done);
        s3.getBucketVersioning(params, (error, data) => {
            expect(error).toBe(null);
            assert.deepStrictEqual(data, { Status: 'Enabled' });
            done();
        });
    });
});


describe('bucket versioning for ingestion buckets', () => {
    const Bucket = `ingestion-bucket-${Date.now()}`;
    beforeAll(done => s3.createBucket({
            Bucket,
            CreateBucketConfiguration: {
                LocationConstraint: 'us-east-2:ingest',
            },
        }, done));

    afterAll(done => s3.deleteBucket({ Bucket }, done));

    test('should not allow suspending versioning for ingestion buckets', done => {
        s3.putBucketVersioning({ Bucket, VersioningConfiguration: {
            Status: 'Suspended'
        } }, err => {
            expect(err).toBeTruthy();
            expect(err.code).toBe('InvalidBucketState');
            done();
        });
    });
});
