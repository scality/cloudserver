'use strict'
const AWS = require('aws-sdk');
const async = require('async');
const s3 = new AWS.S3({
    accessKeyId: 'GN20IUQ621RW2YEBBUX1',
    secretAccessKey: 'l09PpDBkZWRp72enn+AxQ8PNF2FquyGRv=/DTA+Z',
    region: 'us-west-1',
    sslEnabled: false,
    endpoint: 'http://localhost:8000',
    s3ForcePathStyle: true,
    apiVersions: { s3: '2006-03-01' },
    signatureVersion: 'v4',
    signatureCache: false,
});

const second = 1;
const minute = second * 60;
const fifteenMinutes = minute * 15;
const hour = minute * 60;
const day = hour * 24;

// Interval must be in milliseconds.
const interval = second * 1000;
// The current second that counts up to the `timeOut` value.
const timeOut = minute;
let currentSecond = 0;

function awsOperations(callback) {
    const Bucket = `bucket-${currentSecond}`;
    return async.waterfall([
        next => s3.createBucket({ Bucket }, err => next(err)),
        next => async.times(80, (n, cb) =>
            s3.putObject({
                Bucket,
                Key: `object-${currentSecond}-${n}`,
                Body: Buffer.alloc(1),
            }, err => cb(err)),
        err => next(err)),
        next => async.times(20, (n, cb) =>
            s3.getObject({
                Bucket,
                Key: `object-${currentSecond}-${n}`,
            }, err => cb(err)),
        err => next(err)),
    ], err => callback(err));
}

function log(err, isLast) {
    if (err) {
        return process.stdout.write(`Error: ${err}`);
    }
    // Log the number of operations that have occurred.
    const count = currentSecond % 15 === 0 ? `${currentSecond}` : '';
    const end = isLast ? '\nComplete\n' : '';
    return process.stdout.write(`.${count}${end}`);
}

// Perform all the actions once per `interval`, until the `timeout` has passed.
const countDown = setInterval(() => {
    if (++currentSecond === timeOut) {
        clearInterval(countDown);
        return awsOperations(err => log(err, true));
    }
    return awsOperations(log);
}, interval);
