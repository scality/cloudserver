const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
    PutObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const objectKey = `somekey-${genUniqID()}`;
const badObjectKey = `nonexistingkey-${genUniqID()}`;

describe('GCP: DELETE Object', function testSuite() {
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
        await gcpRetry(
            gcpClient,
            new DeleteBucketCommand({ Bucket: bucketName }),
        );
    });

    describe('with existing object in bucket', () => {
        beforeEach(async () => {
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: objectKey,
            });
            await gcpClient.send(cmd);
        });

        it('should successfully delete object', done => {
            async.waterfall([
                next => gcpClient.deleteObject({
                    Bucket: bucketName,
                    Key: objectKey,
                }, err => {
                    assert.equal(err, null,
                        `Expected success, got error ${err}`);
                    return next();
                }),
                next => {
                    const cmd = new GetObjectCommand({
                        Bucket: bucketName,
                        Key: objectKey,
                    });
                    gcpClient.send(cmd)
                        .then(() => {
                            // Should not succeed
                            assert.fail('Expected NoSuchKey error');
                        })
                        .catch(err => {
                            assert(err);
                            assert.strictEqual(
                                err.$metadata && err.$metadata.httpStatusCode,
                                404);
                            assert.strictEqual(err.name, 'NoSuchKey');
                            return next();
                        });
                },
            ], err => done(err));
        });
    });

    describe('without existing object in bucket', () => {
        it('should return 404 and NoSuchKey', done => {
            gcpClient.deleteObject({
                Bucket: bucketName,
                Key: badObjectKey,
            }, err => {
                assert(err);
                assert.strictEqual(err.$metadata.httpStatusCode, 404);
                assert.strictEqual(err.name, 'NoSuchKey');
                return done();
            });
        });
    });
});
