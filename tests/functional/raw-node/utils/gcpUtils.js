const async = require('async');
const assert = require('assert');

const { makeGcpRequest } = require('./makeRequest');

function gcpRequestRetry(params, retry, callback) {
    const maxRetries = 4;
    const timeout = Math.pow(2, retry) * 1000;
    return setTimeout(makeGcpRequest, timeout, params, (err, res) => {
        if (err) {
            if (retry <= maxRetries && err.statusCode === 429) {
                return gcpRequestRetry(params, retry + 1, callback);
            }
            return callback(err);
        }
        return callback(null, res);
    });
}

function gcpClientRetry(fn, params, callback, retry = 0) {
    const maxRetries = 4;
    const timeout = Math.pow(2, retry) * 1000;
    return setTimeout(fn, timeout, params, (err, res) => {
        if (err) {
            if (retry <= maxRetries && err.statusCode === 429) {
                return gcpClientRetry(fn, params, callback, retry + 1);
            }
            return callback(err);
        }
        return callback(null, res);
    });
}

// mpu test helpers
function gcpMpuSetup(params, callback) {
    const { gcpClient, bucketNames, key, partCount, partSize } = params;
    return async.waterfall([
        next => gcpClient.createMultipartUpload({
            Bucket: bucketNames.mpu.Name,
            Key: key,
        }, (err, res) => {
            assert.equal(err, null,
                `Expected success, but got error ${err}`);
            return next(null, res.UploadId);
        }),
        (uploadId, next) => {
            if (partCount <= 0) {
                return next('SkipPutPart', { uploadId });
            }
            const arrayData = Array.from(Array(partCount).keys());
            const etagList = Array(partCount);
            return async.each(arrayData,
            (info, moveOn) => {
                gcpClient.uploadPart({
                    Bucket: bucketNames.mpu.Name,
                    Key: key,
                    UploadId: uploadId,
                    PartNumber: info + 1,
                    Body: Buffer.alloc(partSize),
                    ContentLength: partSize,
                }, (err, res) => {
                    if (err) {
                        return moveOn(err);
                    }
                    etagList[info] = res.ETag;
                    return moveOn(null);
                });
            }, err => {
                next(err, { uploadId, etagList });
            });
        },
    ], (err, result) => {
        if (err) {
            if (err === 'SkipPutPart') {
                return callback(null, result);
            }
            return callback(err);
        }
        return callback(null, result);
    });
}

/*
<CreateBucketConfiguration>
  <LocationConstraint><location></LocationConstraint>
  <StorageClass><storage class></StorageClass>
</CreateBucketConfiguration>
*/
const regionalLoc = 'us-west1';
const multiRegionalLoc = 'us';
function setBucketClass(storageClass) {
    const locationConstraint =
        storageClass === 'REGIONAL' ? regionalLoc : multiRegionalLoc;
    return '<CreateBucketConfiguration>' +
        `<LocationConstraint>${locationConstraint}</LocationConstraint>` +
        `<StorageClass>${storageClass}</StorageClass>` +
        '</CreateBucketConfiguration>';
}

module.exports = {
    gcpRequestRetry,
    gcpClientRetry,
    setBucketClass,
    gcpMpuSetup,
};
