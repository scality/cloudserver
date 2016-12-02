import { S3 } from 'aws-sdk';
import { times, timesSeries, waterfall } from 'async';

import getConfig from '../support/config';

const text = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890';
const bucketCount = 100;
const objectCount = 100;
const loopCount = 100;

function generateRandomName(length) {
    const count = length || 15;
    let name = '';
    for (let i = 0; i < count; i++) {
        name += String.fromCharCode(Math.floor(Math.random() * 92 + 33));
    }
    return name;
}

function generateBucketName() {
    return `stress-test-bucket-${generateRandomName()}-${Date.now()}`;
}

function putObjects(s3, loopId, bucket, cb) {
    times(objectCount, (i, next) => {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}`, Body: text };
        s3.putObject(params, next);
    }, cb);
}

function deleteObjects(s3, loopId, bucket, cb) {
    times(objectCount, (i, next) => {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}` };
        s3.deleteObject(params, next);
    }, cb);
}

describe('aws-node-sdk stress test bucket', function testSuite() {
    this.timeout(120000);
    let s3;
    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    it('createBucket-putObject-deleteObject-deleteBucket loop', done =>
        times(bucketCount, (i, next) => {
            const bucket = generateBucketName();
            timesSeries(loopCount, (loopId, next) => waterfall([
                next => s3.createBucket({ Bucket: bucket }, err => next(err)),
                next => putObjects(s3, loopId, bucket, err => next(err)),
                next => deleteObjects(s3, loopId, bucket, err => next(err)),
                next => s3.deleteBucket({ Bucket: bucket }, err => next(err)),
            ], err => next(err)), next);
        }, done)
    );
});
