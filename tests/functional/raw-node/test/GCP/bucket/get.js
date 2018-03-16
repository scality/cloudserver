const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { listingHardLimit } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${Date.now()}`;
const smallSize = 20;
const bigSize = listingHardLimit + 1;
const config = getRealAwsConfig(credentialOne);
const gcpClient = new GCP(config);

function populateBucket(createdObjects, callback) {
    process.stdout.write(
        `Putting ${createdObjects.length} objects into bucket\n`);
    async.mapLimit(createdObjects, 10,
    (object, moveOn) => {
        makeGcpRequest({
            method: 'PUT',
            bucket: bucketName,
            objectKey: object,
            authCredentials: config.credentials,
        }, err => moveOn(err));
    }, err => {
        if (err) {
            process.stdout
                .write(`err putting objects ${err.code}`);
        }
        return callback(err);
    });
}

function removeObjects(createdObjects, callback) {
    process.stdout.write(
        `Deleting ${createdObjects.length} objects from bucket\n`);
    async.mapLimit(createdObjects, 10,
    (object, moveOn) => {
        makeGcpRequest({
            method: 'DELETE',
            bucket: bucketName,
            objectKey: object,
            authCredentials: config.credentials,
        }, err => moveOn(err));
    }, err => {
        if (err) {
            process.stdout
                .write(`err deleting objects ${err.code}`);
        }
        return callback(err);
    });
}

describe('GCP: GET Bucket', function testSuite() {
    this.timeout(180000);

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

    describe('without existing bucket', () => {
        it('should return 404 and NoSuchBucket', done => {
            const badBucketName = `nonexistingbucket-${Date.now()}`;
            gcpClient.getBucket({
                Bucket: badBucketName,
            }, err => {
                assert(err);
                assert.strictEqual(err.statusCode, 404);
                assert.strictEqual(err.code, 'NoSuchBucket');
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        describe('with less than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(smallSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));

            after(done => removeObjects(createdObjects, done));

            it(`should list all ${smallSize} created objects`, done => {
                gcpClient.listObjects({
                    Bucket: bucketName,
                }, (err, res) => {
                    assert.equal(err, null, `Expected success, but got ${err}`);
                    assert.strictEqual(res.Contents.length, smallSize);
                    return done();
                });
            });

            describe('with MaxKeys at 10', () => {
                it('should list MaxKeys number of objects', done => {
                    gcpClient.listObjects({
                        Bucket: bucketName,
                        MaxKeys: 10,
                    }, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got ${err}`);
                        assert.strictEqual(res.Contents.length, 10);
                        return done();
                    });
                });
            });
        });

        describe('with more than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(bigSize).keys()).map(i => `someObject-${i}`);

            before(done => populateBucket(createdObjects, done));

            after(done => removeObjects(createdObjects, done));

            it('should list at max 1000 of objects created', done => {
                gcpClient.listObjects({
                    Bucket: bucketName,
                }, (err, res) => {
                    assert.equal(err, null, `Expected success, but got ${err}`);
                    assert.strictEqual(res.Contents.length,
                        listingHardLimit);
                    return done();
                });
            });

            describe('with MaxKeys at 1001', () => {
                it('should list at max 1000, ignoring MaxKeys', done => {
                    gcpClient.listObjects({
                        Bucket: bucketName,
                        MaxKeys: 1001,
                    }, (err, res) => {
                        assert.equal(err, null,
                            `Expected success, but got ${err}`);
                        assert.strictEqual(res.Contents.length,
                            listingHardLimit);
                        return done();
                    });
                });
            });
        });
    });
});
