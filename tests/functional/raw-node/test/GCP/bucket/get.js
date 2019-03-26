const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { makeGcpRequest } = require('../../../utils/makeRequest');
const { gcpRequestRetry, genUniqID } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const { listingHardLimit } = require('../../../../../../constants');

const credentialOne = 'gcpbackend';
const bucketName = `somebucket-${genUniqID()}`;
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

describe('GCP: GET Bucket', () => {
    this.timeout(180000);

    beforeAll(done => {
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

    afterAll(done => {
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
        test('should return 404 and NoSuchBucket', done => {
            const badBucketName = `nonexistingbucket-${genUniqID()}`;
            gcpClient.getBucket({
                Bucket: badBucketName,
            }, err => {
                expect(err).toBeTruthy();
                expect(err.statusCode).toBe(404);
                expect(err.code).toBe('NoSuchBucket');
                return done();
            });
        });
    });

    describe('with existing bucket', () => {
        describe('with less than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(smallSize).keys()).map(i => `someObject-${i}`);

            beforeAll(done => populateBucket(createdObjects, done));

            afterAll(done => removeObjects(createdObjects, done));

            test(`should list all ${smallSize} created objects`, done => {
                gcpClient.listObjects({
                    Bucket: bucketName,
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.Contents.length).toBe(smallSize);
                    return done();
                });
            });

            describe('with MaxKeys at 10', () => {
                test('should list MaxKeys number of objects', done => {
                    gcpClient.listObjects({
                        Bucket: bucketName,
                        MaxKeys: 10,
                    }, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.Contents.length).toBe(10);
                        return done();
                    });
                });
            });
        });

        describe('with more than listingHardLimit number of objects', () => {
            const createdObjects = Array.from(
                Array(bigSize).keys()).map(i => `someObject-${i}`);

            beforeAll(done => populateBucket(createdObjects, done));

            afterAll(done => removeObjects(createdObjects, done));

            test('should list at max 1000 of objects created', done => {
                gcpClient.listObjects({
                    Bucket: bucketName,
                }, (err, res) => {
                    expect(err).toEqual(null);
                    expect(res.Contents.length).toBe(listingHardLimit);
                    return done();
                });
            });

            describe('with MaxKeys at 1001', () => {
                test('should list at max 1000, ignoring MaxKeys', done => {
                    gcpClient.listObjects({
                        Bucket: bucketName,
                        MaxKeys: 1001,
                    }, (err, res) => {
                        expect(err).toEqual(null);
                        expect(res.Contents.length).toBe(listingHardLimit);
                        return done();
                    });
                });
            });
        });
    });
});
