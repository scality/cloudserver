const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GetBucketVersioningCommand } = require('@aws-sdk/client-s3');
const { GCP } = arsenal.storage.data.external.GCP;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const verEnabledObj = 'Enabled';
const verDisabledObj = 'Suspended';
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
        this.currentTest.bucketName = `somebucket-${genUniqID()}`;
        gcpRequestRetry({
            method: 'PUT',
            bucket: this.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err.code}\n`);
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
                process.stdout.write(`err in deleting bucket ${err.code}\n`);
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
                queryObj: { versioning: '' },
                requestBody: xmlEnable,
            }, err => {
                if (err) {
                    process.stdout.write(`err in setting versioning ${err.code}`);
                }
                return next(err);
            }),
            next => {
                const command = new GetBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                });
                return gcpClient.send(command)
                    .then(res => {
                        assert.deepStrictEqual(res.Status, verEnabledObj);
                        return next();
                    })
                    .catch(err => {
                        assert.equal(err, null,
                            `Expected success, but got err ${err}`);
                        return next(err);
                    });
            },
        ], err => done(err));
    });

    it('should verify bucket versioning is disabled', function testFn(done) {
        return async.waterfall([
            next => makeGcpRequest({
                method: 'PUT',
                bucket: this.test.bucketName,
                authCredentials: config.credentials,
                queryObj: { versioning: '' },
                requestBody: xmlDisable,
            }, err => {
                if (err) {
                    process.stdout.write(`err in setting versioning ${err}`);
                }
                return next(err);
            }),
            next => {
                const command = new GetBucketVersioningCommand({
                    Bucket: this.test.bucketName,
                });
                return gcpClient.send(command)
                    .then(res => {
                        assert.deepStrictEqual(res.Status, verDisabledObj);
                        return next();
                    })
                    .catch(err => {
                        assert.equal(err, null,
                            `Expected success, but got err ${err}`);
                        return next(err);
                    });
            },
        ], err => done(err));
    });
});
