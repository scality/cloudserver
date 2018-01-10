const assert = require('assert');
const async = require('async');
const { GCP } = require('../../../../../../lib/data/external/GCP');
const { gcpRequestRetry, setBucketClass } = require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${Date.now()}`,
        Type: 'REGIONAL',
    },
    overflow: {
        Name: `overflowbucket-${Date.now()}`,
        Type: 'MULTI_REGIONAL',
    },
};

const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const smallMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

describe('GCP: Upload Object', function testSuite() {
    this.timeout(600000);
    let config;
    let gcpClient;

    before(done => {
        config = getRealAwsConfig(credentialOne);
        gcpClient = new GCP(config);
        async.eachSeries(bucketNames,
            (bucket, next) => gcpRequestRetry({
                method: 'PUT',
                bucket: bucket.Name,
                authCredentials: config.credentials,
                requestBody: setBucketClass(bucket.Type),
            }, 0, err => {
                if (err) {
                    process.stdout.write(`err in creating bucket ${err}\n`);
                }
                return next(err);
            }),
        err => done(err));
    });

    after(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
                assert.equal(err, null,
                    `Expected success, but got error ${err}`);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    assert.equal(err, null,
                        `Expected success, but got error ${err}`);
                    gcpRequestRetry({
                        method: 'DELETE',
                        bucket: bucket.Name,
                        authCredentials: config.credentials,
                    }, 0, err => {
                        if (err) {
                            process.stdout.write(
                                `err in deleting bucket ${err}\n`);
                        }
                        return next(err);
                    });
                });
            }),
        err => done(err));
    });

    it('should put an object to GCP', done => {
        const key = `somekey-${Date.now()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Overflow: bucketNames.overflow.Name,
            Key: key,
            Body: body,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, got error ${err}`);
            assert.strictEqual(res.ETag, `"${smallMD5}"`);
            return done();
        });
    });

    it('should put a large object to GCP', done => {
        const key = `somekey-${Date.now()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Overflow: bucketNames.overflow.Name,
            Key: key,
            Body: bigBody,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, got error ${err}`);
            assert.strictEqual(res.ETag, `"${bigMD5}"`);
            return done();
        });
    });
});
