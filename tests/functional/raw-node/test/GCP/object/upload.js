const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external.GCP;
const { genUniqID, gcpRetry, listBucketObjects } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');
const {
    CreateBucketCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${genUniqID()}`,
    },
    mpu: {
        Name: `mpubucket-${genUniqID()}`,
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
        async.eachSeries(
            Object.values(bucketNames),
            (bucket, next) => gcpRetry(
                gcpClient,
                () => new CreateBucketCommand({ Bucket: bucket.Name }),
                null,
                next,
            ),
            err => done(err),
        );
    });

    after(done => {
        async.eachSeries(
            Object.values(bucketNames),
            (bucket, next) => listBucketObjects(
                gcpClient,
                { Bucket: bucket.Name },
                (err, res) => {
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
                    gcpRetry(
                        gcpClient,
                        () => new DeleteBucketCommand({ Bucket: bucket.Name }),
                        null,
                        error => {
                            if (error) {
                                process.stdout.write(
                                    `err in deleting bucket ${error}\n`);
                            }
                            return next(error);
                        },
                    );
                });
            }),
            err => done(err),
        );
    });

    it('should put an object to GCP', done => {
        const key = `somekey-${genUniqID()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
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
        const key = `somekey-${genUniqID()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
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
