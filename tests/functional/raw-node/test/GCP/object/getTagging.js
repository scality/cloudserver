const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genGetTagObj, genUniqID, genBucketName, gcpRetry } =
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
const bucketName = genBucketName('gettagging');
const tagSize = 10;

describe('GCP: GET Object Tagging', function testSuite() {
    this.timeout(120000);
    let config;
    let gcpClient;
    let bucketCreated = false;

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        await gcpRetry(
            gcpClient,
            new CreateBucketCommand({ Bucket: bucketName }),
        );
        bucketCreated = true;
    });

    beforeEach(async function beforeFn() {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const { expectedTagObj } =
            genGetTagObj(tagSize, `x-goog-meta-${gcpTaggingPrefix}`);
        this.currentTest.tagObj = expectedTagObj;

        const putCmd = new PutObjectCommand({
            Bucket: bucketName,
            Key: this.currentTest.key,
        });

        const res = await gcpClient.send(putCmd);
        this.currentTest.versionId = res.VersionId;

        await new Promise((resolve, reject) => {
            gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.currentTest.key,
                VersionId: this.currentTest.versionId,
                Tagging: {
                    TagSet: this.currentTest.tagObj,
                },
            }, err => {
                if (err) {
                    process.stdout
                        .write(`err in setting object tags ${err}`);
                    reject(err);
                    return;
                }
                resolve();
            });
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

    after(async () => {
        if (!bucketCreated) {
            return;
        }
        await gcpRetry(
            gcpClient,
            new DeleteBucketCommand({ Bucket: bucketName }),
        );
    });

    it('should successfully get object tags', function testFn(done) {
        gcpClient.getObjectTagging({
            Bucket: bucketName,
            Key: this.test.key,
            VersionId: this.test.versionId,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, got error ${err}`);
            assert.deepStrictEqual(res.TagSet, this.test.tagObj);
            return done();
        });
    });
});
