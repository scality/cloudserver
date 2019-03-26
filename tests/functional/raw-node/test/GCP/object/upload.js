const assert = require('assert');
const async = require('async');
const arsenal = require('arsenal');
const { GCP } = arsenal.storage.data.external;
const { gcpRequestRetry, setBucketClass, genUniqID } =
    require('../../../utils/gcpUtils');
const { getRealAwsConfig } =
    require('../../../../aws-node-sdk/test/support/awsConfig');

const credentialOne = 'gcpbackend';
const bucketNames = {
    main: {
        Name: `somebucket-${genUniqID()}`,
        Type: 'MULTI_REGIONAL',
    },
    mpu: {
        Name: `mpubucket-${genUniqID()}`,
        Type: 'MULTI_REGIONAL',
    },
};

const body = Buffer.from('I am a body', 'utf8');
const bigBody = Buffer.alloc(10485760);
const smallMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const bigMD5 = 'a7d414b9133d6483d9a1c4e04e856e3b-2';

describe('GCP: Upload Object', () => {
    this.timeout(600000);
    let config;
    let gcpClient;

    beforeAll(done => {
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

    afterAll(done => {
        async.eachSeries(bucketNames,
            (bucket, next) => gcpClient.listObjects({
                Bucket: bucket.Name,
            }, (err, res) => {
                expect(err).toEqual(null);
                async.map(res.Contents, (object, moveOn) => {
                    const deleteParams = {
                        Bucket: bucket.Name,
                        Key: object.Key,
                    };
                    gcpClient.deleteObject(
                        deleteParams, err => moveOn(err));
                }, err => {
                    expect(err).toEqual(null);
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

    test('should put an object to GCP', done => {
        const key = `somekey-${genUniqID()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Key: key,
            Body: body,
        }, (err, res) => {
            expect(err).toEqual(null);
            expect(res.ETag).toBe(`"${smallMD5}"`);
            return done();
        });
    });

    test('should put a large object to GCP', done => {
        const key = `somekey-${genUniqID()}`;
        gcpClient.upload({
            Bucket: bucketNames.main.Name,
            MPU: bucketNames.mpu.Name,
            Key: key,
            Body: bigBody,
        }, (err, res) => {
            expect(err).toEqual(null);
            expect(res.ETag).toBe(`"${bigMD5}"`);
            return done();
        });
    });
});
