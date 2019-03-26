const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genGetTagObj, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const gcpTagPrefix = `x-goog-meta-${gcpTaggingPrefix}`;
const tagSize = 10;

describe('GCP: GET Object Tagging', () => {
    let testContext;

    beforeEach(() => {
        testContext = {};
    });

    let config;
    let gcpClient;

    beforeAll(done => {
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

    beforeEach(done => {
        testContext.currentTest.key = `somekey-${genUniqID()}`;
        testContext.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const { tagHeader, expectedTagObj } =
            genGetTagObj(tagSize, gcpTagPrefix);
        testContext.currentTest.tagObj = expectedTagObj;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: testContext.currentTest.key,
            authCredentials: config.credentials,
            headers: tagHeader,
        }, (err, res) => {
            if (err) {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            }
            testContext.currentTest.versionId = res.headers['x-goog-generation'];
            return done();
        });
    });

    afterEach(done => {
        makeGcpRequest({
            method: 'DELETE',
            bucket: bucketName,
            objectKey: testContext.currentTest.key,
            authCredentials: config.credentials,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting object ${err}`);
            }
            return done(err);
        });
    });

    afterAll(done => {
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

    test('should successfully get object tags', done => {
        gcpClient.getObjectTagging({
            Bucket: bucketName,
            Key: testContext.test.key,
            VersionId: testContext.test.versionId,
        }, (err, res) => {
            expect(err).toEqual(null);
            assert.deepStrictEqual(res.TagSet, testContext.test.tagObj);
            return done();
        });
    });
});
