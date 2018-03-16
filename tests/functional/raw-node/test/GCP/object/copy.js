const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${Date.now()}`;

describe('GCP: COPY Object', function testSuite() {
    this.timeout(180000);
    const config = getRealAwsConfig(credentialOne);
    const gcpClient = new GCP(config);

    before(done => {
        gcpRequestRetry({
            method: 'PUT',
            bucket: bucketName,
            authCredentials: config.credentials,
        }, 0, err => {
            if (err) {
                process.stdout.write(`err in creating bucket ${err}\n`);
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
                process.stdout.write(`err in creating bucket ${err}\n`);
            }
            return done(err);
        });
    });

    describe('without existing object in bucket', () => {
        it('should return 404 and \'NoSuchKey\'', done => {
            const missingObject = `nonexistingkey-${Date.now()}`;
            const someKey = `somekey-${Date.now()}`;
            gcpClient.copyObject({
                Bucket: bucketName,
                Key: someKey,
                CopySource: `/${bucketName}/${missingObject}`,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            });
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            this.currentTest.copyKey = `copykey-${Date.now()}`;
            this.currentTest.initValue = `${Date.now()}`;
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey: this.currentTest.copyKey,
                headers: {
                    'x-goog-meta-value': this.currentTest.initValue,
                },
                authCredentials: config.credentials,
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                }
                this.currentTest.contentHash = res.headers['x-goog-hash'];
                return done(err);
            });
        });

        afterEach(function afterFn(done) {
            async.parallel([
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
                    objectKey: this.currentTest.key,
                    authCredentials: config.credentials,
                }, err => {
                    if (err) {
                        process.stdout.write(`err in deleting object ${err}\n`);
                    }
                    return next(err);
                }),
                next => makeGcpRequest({
                    method: 'DELETE',
                    bucket: bucketName,
                    objectKey: this.currentTest.copyKey,
                    authCredentials: config.credentials,
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
            const newValue = `${Date.now()}`;
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
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    assert.strictEqual(this.test.contentHash,
                        res.headers['x-goog-hash']);
                    assert.notStrictEqual(res.headers['x-goog-meta-value'],
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
                next => makeGcpRequest({
                    method: 'HEAD',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, res) => {
                    if (err) {
                        process.stdout
                            .write(`err in retrieving object ${err}\n`);
                        return next(err);
                    }
                    assert.strictEqual(this.test.contentHash,
                        res.headers['x-goog-hash']);
                    assert.strictEqual(res.headers['x-goog-meta-value'],
                        this.test.initValue);
                    return next();
                }),
            ], done);
        });
    });
});
