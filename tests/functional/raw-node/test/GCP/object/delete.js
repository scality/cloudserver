const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
const objectKey = `somekey-${genUniqID()}`;
const badObjectKey = `nonexistingkey-${genUniqID()}`;

describe('GCP: DELETE Object', function testSuite() {
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

    describe('with existing object in bucket', () => {
        beforeEach(done => {
            makeGcpRequest({
                method: 'PUT',
                bucket: bucketName,
                objectKey,
                authCredentials: config.credentials,
            }, err => {
                if (err) {
                    process.stdout.write(`err in creating object ${err}\n`);
                }
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
                next => makeGcpRequest({
                    method: 'GET',
                    bucket: bucketName,
                    objectKey,
                    authCredentials: config.credentials,
                }, err => {
                    assert(err);
                    assert.strictEqual(err.statusCode, 404);
                    assert.strictEqual(err.code, 'NoSuchKey');
                    return next();
                }),
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
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchKey');
                return done();
            });
        });
    });
});
