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
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;

describe('GCP: COPY Object', function testSuite() {
    this.timeout(180000);
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
                process.stdout.write(`err in creating bucket ${err}\n`);
                return done(err);
            });
    });

    describe('without existing object in bucket', () => {
        it('should return 404 and \'NoSuchKey\'', done => {
            const missingObject = `nonexistingkey-${genUniqID()}`;
            const someKey = `somekey-${genUniqID()}`;
            gcpClient.copyObject({
                Bucket: bucketName,
                Key: someKey,
                CopySource: `/${bucketName}/${missingObject}`,
            }, err => {
                assert(err);
                assert.strictEqual(err.$metadata.httpStatusCode, 404);
                assert.strictEqual(err.name, 'NoSuchKey');
                return done();
            });
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${genUniqID()}`;
            this.currentTest.copyKey = `copykey-${genUniqID()}`;
            this.currentTest.initValue = `${genUniqID()}`;
            const cmd = new PutObjectCommand({
                Bucket: bucketName,
                Key: this.currentTest.copyKey,
                Metadata: {
                    value: this.currentTest.initValue,
                },
            });
            gcpClient.send(cmd)
                .then(res => {
                    this.currentTest.ETag = res.ETag;
                    return done();
                })
                .catch(err => {
                    process.stdout.write(`err in creating object ${err}\n`);
                    return done(err);
                });
        });

        afterEach(function afterFn(done) {
            async.parallel([
                next => gcpClient.deleteObject({
                    Bucket: bucketName,
                    Key: this.currentTest.key,
                }, err => {
                    if (err) {
                        process.stdout.write(`err in deleting object ${err}\n`);
                    }
                    return next(err);
                }),
                next => gcpClient.deleteObject({
                    Bucket: bucketName,
                    Key: this.currentTest.copyKey,
                }, err => {
                    if (err) {
                        process.stdout
                            .write(`err in deleting copy object ${err}\n`);
                    }
                    return next(err);
                }),
            ], done);
        });

        it('should successfully copy with REPLACE directive',
        function testFn(done) {
            const newValue = `${genUniqID()}`;
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                    CopySource: `/${bucketName}/${this.test.copyKey}`,
                    MetadataDirective: 'REPLACE',
                    Metadata: {
                        value: newValue,
                    },
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    return next();
                }),
                next => gcpClient.headObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    assert.strictEqual(res.ETag, this.test.ETag);
                    assert.notStrictEqual(res.Metadata.value,
                        this.test.initValue);
                    return next();
                }),
            ], done);
        });

        it('should successfully copy with COPY directive',
        function testFn(done) {
            async.waterfall([
                next => gcpClient.copyObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                    CopySource: `/${bucketName}/${this.test.copyKey}`,
                    MetadataDirective: 'COPY',
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    return next();
                }),
                next => gcpClient.headObject({
                    Bucket: bucketName,
                    Key: this.test.key,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    assert.strictEqual(res.ETag, this.test.ETag);
                    assert.strictEqual(res.Metadata.value,
                        this.test.initValue);
                    return next();
                }),
            ], done);
        });
    });
});
