const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genDelTagObj, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const gcpTagPrefix = `x-goog-meta-${gcpTaggingPrefix}`;
let config;
let gcpClient;

function assertObjectMetaTag(params, callback) {
    return makeGcpRequest({
        method: 'HEAD',
        bucket: params.bucket,
        objectKey: params.key,
        authCredentials: config.credentials,
        headers: {
            'x-goog-generation': params.versionId,
        },
    }, (err, res) => {
        if (err) {
            process.stdout.write(`err in retrieving object ${err}`);
            return callback(err);
        }
        const resObj = res.headers;
        const tagRes = {};
        Object.keys(resObj).forEach(
        header => {
            if (header.startsWith(gcpTagPrefix)) {
                tagRes[header] = resObj[header];
                delete resObj[header];
            }
        });
        const metaRes = {};
        Object.keys(resObj).forEach(
        header => {
            if (header.startsWith('x-goog-meta-')) {
                metaRes[header] = resObj[header];
                delete resObj[header];
            }
        });
        assert.deepStrictEqual(params.tag, tagRes);
        assert.deepStrictEqual(params.meta, metaRes);
        return callback();
    });
}

describe('GCP: DELETE Object Tagging', function testSuite() {
    this.timeout(30000);

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}`);
            }
            return done(err);
        });
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const { headers, expectedTagObj, expectedMetaObj } =
            genDelTagObj(10, gcpTagPrefix);
        this.currentTest.expectedTagObj = expectedTagObj;
        this.currentTest.expectedMetaObj = expectedMetaObj;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: this.currentTest.key,
            authCredentials: config.credentials,
            headers,
        }, (err, res) => {
            if (err) {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            }
            this.currentTest.versionId = res.headers['x-goog-generation'];
            return done();
        });
    });

    afterEach(function afterFn(done) {
        makeGcpRequest({
            method: 'DELETE',
            bucket: bucketName,
            objectKey: this.currentTest.key,
            authCredentials: config.credentials,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting object ${err}`);
            }
            return done(err);
        });
    });

    after(done => {
        gcpRequestRetry({
            method: 'DELETE',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in deleting bucket ${err}`);
            }
            return done(err);
        });
    });

    it('should successfully delete object tags', function testFn(done) {
        async.waterfall([
            next => assertObjectMetaTag({
                bucket: bucketName,
                key: this.test.key,
                versionId: this.test.versionId,
                meta: this.test.expectedMetaObj,
                tag: this.test.expectedTagObj,
            }, next),
            next => gcpClient.deleteObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
            }, err => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                return next();
            }),
            next => assertObjectMetaTag({
                bucket: bucketName,
                key: this.test.key,
                versionId: this.test.versionId,
                meta: this.test.expectedMetaObj,
                tag: {},
            }, next),
        ], done);
    });
});
