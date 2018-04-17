const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genPutTagObj, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { gcpTaggingPrefix } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const gcpTagPrefix = `x-goog-meta-${gcpTaggingPrefix}`;

describe('GCP: PUT Object Tagging', () => {
    let config;
    let gcpClient;

    before(done => {
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

    beforeEach(function beforeFn(done) {
        this.currentTest.key = `somekey-${genUniqID()}`;
        this.currentTest.specialKey = `veryspecial-${genUniqID()}`;
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: this.currentTest.key,
            authCredentials: config.credentials,
        }, (err, res) => {
            if (err) {
                process.stdout.write(`err in creating object ${err}`);
                return done(err);
            }
            this.currentTest.versionId = res.headers['x-goog-generation'];
            return done();
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
                process.stdout.write(`err in deleting object ${err}`);
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
                process.stdout.write(`err in deleting bucket ${err}`);
            }
            return done(err);
        });
    });

    it('should successfully put object tags', function testFn(done) {
        async.waterfall([
            next => gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: [
                        {
                            Key: this.test.specialKey,
                            Value: this.test.specialKey,
                        },
                    ],
                },
            }, err => {
                assert.equal(err, null,
                    `Expected success, got error ${err}`);
                return next();
            }),
            next => makeGcpRequest({
                method: 'HEAD',
                bucket: bucketName,
                objectKey: this.test.key,
                authCredentials: config.credentials,
                headers: {
                    'x-goog-generation': this.test.versionId,
                },
            }, (err, res) => {
                if (err) {
                    process.stdout.write(`err in retrieving object ${err}`);
                    return next(err);
                }
                const toCompare =
                    res.headers[`${gcpTagPrefix}${this.test.specialKey}`];
                assert.strictEqual(toCompare, this.test.specialKey);
                return next();
            }),
        ], done);
    });

    describe('when tagging parameter is incorrect', () => {
        it('should return 400 and BadRequest if more than ' +
        '10 tags are given', function testFun(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: genPutTagObj(11),
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'BadRequest');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given duplicate keys',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: genPutTagObj(10, true),
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given invalid key',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: [
                        { Key: Buffer.alloc(129, 'a'), Value: 'bad tag' },
                    ],
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });

        it('should return 400 and InvalidTag if given invalid value',
        function testFn(done) {
            return gcpClient.putObjectTagging({
                Bucket: bucketName,
                Key: this.test.key,
                VersionId: this.test.versionId,
                Tagging: {
                    TagSet: [
                        { Key: 'badtag', Value: Buffer.alloc(257, 'a') },
                    ],
                },
            }, err => {
                assert(err);
                assert.strictEqual(err.code, 400);
                assert.strictEqual(err.message, 'InvalidTag');
                return done();
            });
        });
    });
});
