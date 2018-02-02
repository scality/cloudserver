const assert = require('assert');
const async = require('async');
const xml2js = require('xml2js');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const verEnabledObj = { VersioningConfiguration: { Status: ['Enabled'] } };
const verDisabledObj = { VersioningConfiguration: { Status: ['Suspended'] } };

function resParseAndAssert(xml, compareObj, callback) {
    return xml2js.parseString(xml, (err, res) => {
        if (err) {
            process.stdout.write(`err in parsing response ${err}\n`);
            return callback(err);
        }
        assert.deepStrictEqual(res, compareObj);
        return callback();
    });
}

describe('GCP: PUT Bucket Versioning', () => {
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
            } else {
                process.stdout.write('Created bucket\n');
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
            } else {
                process.stdout.write('Deleted bucket\n');
            }
            return done(err);
        });
    });

    it('should enable bucket versioning', function testFn(done) {
        return async.waterfall([
            next => gcpClient.putBucketVersioning({
                Bucket: this.test.bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }, err => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                return next();
            }),
            next => makeGcpRequest({
                method: 'GET',
                bucket: this.test.bucketName,
                authCredentials: config.credentials,
                queryObj: { versioning: {} },
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in retrieving bucket ${err}`);
                    return next(err);
                }
                return resParseAndAssert(res.body, verEnabledObj, next);
            }),
        ], err => done(err));
    });

    it('should disable bucket versioning', function testFn(done) {
        return async.waterfall([
            next => gcpClient.putBucketVersioning({
                Bucket: this.test.bucketName,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            }, err => {
                assert.equal(err, null,
                    `Expected success, but got err ${err}`);
                return next();
            }),
            next => makeGcpRequest({
                method: 'GET',
                bucket: this.test.bucketName,
                authCredentials: config.credentials,
                queryObj: { versioning: {} },
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in retrieving bucket ${err}`);
                    return next(err);
                }
                return resParseAndAssert(res.body, verDisabledObj, next);
            }),
        ], err => done(err));
    });
});
