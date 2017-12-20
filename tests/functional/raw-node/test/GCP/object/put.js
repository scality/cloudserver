const assert = require('assert');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${Date.now()}`;

describe('GCP: PUT Object', function testSuite() {
    this.timeout(30000);
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
                process.stdout.write(`err in deleting bucket ${err}\n`);
            }
            return done(err);
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
                process.stdout.write(`err in deleting object ${err}\n`);
            }
            return done(err);
        });
    });

    describe('with existing object in bucket', () => {
        beforeEach(function beforeFn(done) {
            this.currentTest.key = `somekey-${Date.now()}`;
            gcpRequestRetry({
                method: 'PUT',
                bucket: this.currentTest.bucketName,
                objectKey: this.currentTest.key,
                authCredentials: config.credentials,
            }, 0, (err, res) => {
                if (err) {
                    process.stdout.write(`err in putting object ${err}\n`);
                    return done(err);
                }
                this.currentTest.uploadId =
                    res.headers['x-goog-generation'];
                return done();
            });
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
            this.test.key = `somekey-${Date.now()}`;
            gcpClient.putObject({
                Bucket: bucketName,
                Key: this.test.key,
            }, (err, putRes) => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey: this.test.key,
                    authCredentials: config.credentials,
                }, (err, getRes) => {
                    if (err) {
                        process.stdout.write(`err in getting bucket ${err}\n`);
                        return done(err);
                    }
                    assert.strictEqual(getRes.headers['x-goog-generation'],
                        putRes.VersionId);
                    return done();
                });
            });
        });
    });
});
