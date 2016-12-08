import { S3 } from 'aws-sdk';
import { times, timesSeries, waterfall } from 'async';

import getConfig from '../support/config';

const bucket = `stress-test-bucket-${Date.now()}`;
const text = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890';
const objectCount = 100;
const loopCount = 10;

function putObjects(s3, loopId, cb) {
    times(objectCount, (i, next) => {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}`, Body: text };
        s3.putObject(params, next);
    }, cb);
}

function deleteObjects(s3, loopId, cb) {
    times(objectCount, (i, next) => {
        const params = { Bucket: bucket, Key: `foo${loopId}_${i}` };
        s3.deleteObject(params, next);
    }, cb);
}

describe('aws-node-sdk stress test bucket', function testSuite() {
    this.timeout(150000);
    let s3;
    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    it('createBucket-putObject-deleteObject-deleteBucket loop', done =>
        timesSeries(loopCount, (loopId, next) => waterfall([
            next => s3.createBucket({ Bucket: bucket }, err => next(err)),
            next => putObjects(s3, loopId, err => next(err)),
            next => deleteObjects(s3, loopId, err => next(err)),
            next => s3.deleteBucket({ Bucket: bucket }, err => next(err)),
        ], err => next(err)), done)
    );
});
