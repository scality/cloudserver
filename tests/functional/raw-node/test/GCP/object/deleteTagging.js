const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genDelTagObj, genUniqID, gcpRetry } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
let config;
let gcpClient;

function assertObjectMetaTag(params, callback) {
    return gcpClient.headObject({
        Bucket: params.bucket,
        Key: params.key,
        VersionId: params.versionId,
    }, (err, res) => {
        if (err) {
            process.stdout.write(`err in retrieving object ${err}`);
            return callback(err);
        }
        const resMeta = Object.assign({}, res.Metadata || {});
        const tagRes = {};
        const metaRes = {};
        Object.keys(resMeta).forEach(key => {
            if (key.startsWith(gcpTaggingPrefix)) {
                tagRes[key] = resMeta[key];
            } else {
                metaRes[key] = resMeta[key];
            }
        });
        assert.deepStrictEqual(params.tag, tagRes);
        assert.deepStrictEqual(params.meta, metaRes);
        return callback();
    });
}

describe('GCP: DELETE Object Tagging', function testSuite() {
    this.timeout(30000);

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        await gcpRetry(
            gcpClient,
            () => new CreateBucketCommand({ Bucket: bucketName }),
        );
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const { expectedTagObj, expectedMetaObj } =
            genDelTagObj(10, `x-goog-meta-${gcpTaggingPrefix}`);

        const expectedTagMeta = {};
        Object.keys(expectedTagObj).forEach(header => {
            const key = header.replace('x-goog-meta-', '');
            expectedTagMeta[key] = expectedTagObj[header];
        });

        const expectedMetaMeta = {};
        Object.keys(expectedMetaObj).forEach(header => {
            const key = header.replace('x-goog-meta-', '');
            expectedMetaMeta[key] = expectedMetaObj[header];
        });

        this.currentTest.expectedTagObj = expectedTagMeta;
        this.currentTest.expectedMetaObj = expectedMetaMeta;

        const metadata = Object.assign(
            {},
            expectedTagMeta,
            expectedMetaMeta
        );

        const cmd = new PutObjectCommand({
            Bucket: bucketName,
            Key: this.currentTest.key,
            Metadata: metadata,
        });

        gcpClient.send(cmd)
            .then(res => {
                this.currentTest.versionId = res.VersionId;
                return done();
            })
            .catch(err => {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            });
    });

    afterEach(function afterFn(done) {
        gcpClient.deleteObject({
            Bucket: bucketName,
            Key: this.currentTest.key,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting object ${err}`);
            }
            return done(err);
        });
    });

    after(done => {
        gcpRetry(
            gcpClient,
            () => new DeleteBucketCommand({ Bucket: bucketName }),
            null,
            err => {
                if (err) {
                    process.stdout.write(`err in deleting bucket ${err}`);
                }
                return done(err);
            },
        );
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
