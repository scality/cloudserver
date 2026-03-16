const assert = require('assert');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, genBucketName, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = genBucketName('put');

describe('GCP: PUT Object', function testSuite() {
    this.timeout(30000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(async () => {
        await gcpRetry(
            gcpClient,
            new CreateBucketCommand({ Bucket: bucketName }),
        );
    });

    after(async () => {
        await gcpRetry(gcpClient, new DeleteBucketCommand({ Bucket: bucketName }));
    });

    afterEach(function afterFn(done) {
        if (!this.currentTest.key) {
            done();
            return;
        }
        gcpClient.deleteObject({
            Bucket: bucketName,
            Key: this.currentTest.key,
        }, err => {
            if (err) {
                process.stdout.write(`err in deleting object ${err}\n`);
            }
            return done(err);
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(async function beforeFn() {
            this.currentTest.key = `somekey-${genUniqID()}`;
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.key,
            });
            const res = await gcpClient.send(cmd);
            this.currentTest.uploadId = res.VersionId;
        });

        it('should overwrite object', function testFn(done) {
            gcpClient.putObject({
                Bucket: bucketName,
                Key: this.test.key,
            }, (err, res) => {
                assert.notStrictEqual(res.VersionId, this.test.uploadId);
                return done();
            });
        });
    });

    describe('without existing object in bucket', () => {
        it('should successfully put object', function testFn(done) {
            this.test.key = `somekey-${genUniqID()}`;
            gcpClient.putObject({
                Bucket: bucketName,
                Key: this.test.key,
            }, (err, putRes) => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                gcpClient.getObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                }, (getErr, getRes) => {
                    assert.equal(getErr, null,
                        `Expected success, got error ${getErr}`);
                    assert.strictEqual(getRes.VersionId, putRes.VersionId);
                    return done();
                });
            });
        });
    });
});
