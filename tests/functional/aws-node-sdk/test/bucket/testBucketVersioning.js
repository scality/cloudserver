import assert from 'assert';
import { S3 } from 'aws-sdk';

import getConfig from '../support/config';

const bucket = `versioning-bucket-${Date.now()}`;

describe('aws-node-sdk test bucket versioning', function testSuite() {
    this.timeout(60000);
    let s3;

    // setup test
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should not accept empty versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {},
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    it('should retrieve an empty versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, {});
            done();
        });
    });

    it('should not accept versioning configuration w/o \"Status\"', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    it('should retrieve an empty versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, {});
            done();
        });
    });

    it('should not accept versioning configuration w/ invalid value', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                MFADelete: 'fun',
                Status: 'let\'s do it',
            },
        };
        s3.putBucketVersioning(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'IllegalVersioningConfigurationException');
                done();
            } else {
                done('accepted empty versioning configuration');
            }
        });
    });

    it('should retrieve an empty versioning configuration', done => {
        const params = { Bucket: bucket };
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, {});
            done();
        });
    });

    it('should accept valid versioning configuration', done => {
        const params = {
            Bucket: bucket,
            VersioningConfiguration: {
                Status: 'Enabled',
            },
        };
        s3.putBucketVersioning(params, done);
    });

    it('should retrieve the valid versioning configuration', done => {
        const params = { Bucket: bucket };
        // s3.getBucketVersioning(params, done);
        s3.getBucketVersioning(params, (error, data) => {
            assert.strictEqual(error, null);
            assert.deepStrictEqual(data, { Status: 'Enabled' });
            done();
        });
    });
});
