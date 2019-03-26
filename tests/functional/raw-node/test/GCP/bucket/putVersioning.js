const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const xml2js = require('xml2js');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
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
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    beforeEach(done => {
        testContext.currentTest.bucketName = `somebucket-${genUniqID()}`;
        gcpRequestRetry({
            method: 'PUT',
            bucket: testContext.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
            }
            return done(err);
        });
    });

    afterEach(done => {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: testContext.currentTest.bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}\n`);
            }
            return done(err);
        });
    });

    test('should enable bucket versioning', done => {
        return async.waterfall([
            next => gcpClient.putBucketVersioning({
                Bucket: testContext.test.bucketName,
                VersioningConfiguration: {
                    Status: 'Enabled',
                },
            }, err => {
                expect(err).toEqual(null);
                return next();
            }),
            next => makeGcpRequest({
                method: 'GET',
                bucket: testContext.test.bucketName,
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

    test('should disable bucket versioning', done => {
        return async.waterfall([
            next => gcpClient.putBucketVersioning({
                Bucket: testContext.test.bucketName,
                VersioningConfiguration: {
                    Status: 'Suspended',
                },
            }, err => {
                expect(err).toEqual(null);
                return next();
            }),
            next => makeGcpRequest({
                method: 'GET',
                bucket: testContext.test.bucketName,
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
