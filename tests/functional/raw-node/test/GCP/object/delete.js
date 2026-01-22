const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID } = require('../../../utils/gcpUtils');
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

    before(done => {
        const cmd = new CreateBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in creating bucket ${err}\n`);
                return done(err);
            });
    });

    after(done => {
        const cmd = new DeleteBucketCommand({ Bucket: bucketName });
        gcpClient.send(cmd)
            .then(() => done())
            .catch(err => {
                process.stdout.write(`err in deleting bucket ${err}\n`);
                return done(err);
            });
    });

    describe('with existing object in bucket', () => {
        beforeEach(done => {
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: objectKey,
            });
            gcpClient.send(cmd)
                .then(() => done())
                .catch(err => {
                    process.stdout.write(`err in creating object ${err}\n`);
                    return done(err);
                });
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
