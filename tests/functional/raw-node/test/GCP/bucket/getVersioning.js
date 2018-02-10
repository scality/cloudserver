const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const verEnabledObj = { Status: 'Enabled' };
const verDisabledObj = { Status: 'Suspended' };
const xmlEnable =
    '<?xml version="1.0" encoding="UTF-8"?>' +
    '<VersioningConfiguration>' +
    '<Status>Enabled</Status>' +
    '</VersioningConfiguration>';
const xmlDisable =
    '<?xml version="1.0" encoding="UTF-8"?>' +
    '<VersioningConfiguration>' +
    '<Status>Suspended</Status>' +
    '</VersioningConfiguration>';

describe('GCP: GET Bucket Versioning', () => {
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    beforeEach(function beforeFn(done) {
        this.currentTest.bucketName = `somebucket-${Date.now()}`;
        gcpRequestRetry({
            method: 'PUT',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
            }
            return done(err);
        });
    });

    afterEach(function afterFn(done) {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            }
            return done(err);
        });
    });

    it('should verify bucket versioning is enabled', function testFn(done) {
        return async.waterfall([
            next => makeGcpRequest({
                method: 'PUT',
                bucket: this.test.bucketName,
                authCredentials: config.credentials,
                queryObj: { versioning: {} },
                requestBody: xmlEnable,
            }, err => {
                if (err) {
                    process.stdout.write(`err in setting versioning ${err}`);
                }
                return next(err);
            }),
            next => gcpClient.getBucketVersioning({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                assert.deepStrictEqual(res, verEnabledObj);
                return next();
            }),
        ], err => done(err));
    });

    it('should verify bucket versioning is disabled', function testFn(done) {
        return async.waterfall([
            next => makeGcpRequest({
                method: 'PUT',
                bucket: this.test.bucketName,
                authCredentials: config.credentials,
                queryObj: { versioning: {} },
                requestBody: xmlDisable,
            }, err => {
                if (err) {
                    process.stdout.write(`err in setting versioning ${err}`);
                }
                return next(err);
            }),
            next => gcpClient.getBucketVersioning({
                Bucket: this.test.bucketName,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                assert.deepStrictEqual(res, verDisabledObj);
                return next();
            }),
        ], err => done(err));
    });
});
