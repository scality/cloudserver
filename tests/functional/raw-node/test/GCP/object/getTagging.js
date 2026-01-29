const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genGetTagObj, genUniqID, gcpRetry } =
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
const tagSize = 10;

describe('GCP: GET Object Tagging', () => {
    let config;
    let gcpClient;
    let bucketCreated = false;

    before(async () => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        await gcpRetry(
            gcpClient,
            () => new CreateBucketCommand({ Bucket: bucketName }),
        );
        bucketCreated = true;
    });

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        const { expectedTagObj } =
            genGetTagObj(tagSize, `x-goog-meta-${gcpTaggingPrefix}`);
        this.currentTest.tagObj = expectedTagObj;

        const putCmd = new PutObjectCommand({
            Bucket: bucketName,
            Key: this.currentTest.key,
        });

        gcpClient.send(putCmd)
            .then(res => {
                this.currentTest.versionId = res.VersionId;
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
                        return done(err);
                    }
                    return done();
                });
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
        if (!bucketCreated) {
            done();
            return;
        }
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
