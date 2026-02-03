const async = require('async');
const { callbackify } = require('util');
const { v4: uuidv4 } = require('uuid');
const { HeadBucketCommand } = require('@aws-sdk/client-s3');

const genUniqID = () => uuidv4().replace(/-/g, '');

const defaultShouldRetry = err =>
    err && (err.name === 'SlowDown' || err.$metadata?.httpStatusCode === 429);

async function gcpRetryCall(callFn, retryOptions) {
    const {
        maxAttempts = 3,
        shouldRetry = defaultShouldRetry,
        getDelayMs = attempt => Math.pow(2, attempt) * 1000,
    } = retryOptions || {};

    let lastError;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
        try {
             
            return await callFn();
        } catch (err) {
            lastError = err;
            if (!shouldRetry(err, attempt) || attempt === maxAttempts - 1) {
                throw err;
            }
            const delay = getDelayMs(attempt);
            process.stdout.write(
                'Retryable error from GCP, retrying in ' +
                `${delay}ms (attempt ${attempt + 1}): ${err}\n`);
             
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }

    throw lastError;
}

async function gcpRetry(gcpClient, command, retryOptions, cb) {
    if (cb) {
        return callbackify(() => gcpRetry(gcpClient, command,
            retryOptions))(cb);
    }

    return gcpRetryCall(() => gcpClient.send(command), retryOptions);
}

const defaultShouldRetryUpload = err => err && (
    err.name === 'NoSuchBucket'
    || err.name === 'NotFound'
    || err.$metadata?.httpStatusCode === 404
    || err.name === 'SlowDown'
    || err.$metadata?.httpStatusCode === 429
    || (typeof err.message === 'string'
        && (err.message.includes('NoSuchBucket')
            || err.message.includes('unable to complete upload')))
);

const defaultShouldRetryMpuCreate = err => err && (
    err.name === 'NoSuchBucket'
    || err.name === 'NotFound'
    || err.$metadata?.httpStatusCode === 404
    || err.name === 'SlowDown'
    || err.$metadata?.httpStatusCode === 429
);

async function gcpUploadWithRetry(gcpClient, params, retryOptions) {
    const callFn = () => new Promise((resolve, reject) => {
        gcpClient.upload(params, (err, data) => {
            if (err) {
                return reject(err);
            }
            return resolve(data);
        });
    });

    return gcpRetryCall(callFn, {
        maxAttempts: 6,
        shouldRetry: defaultShouldRetryUpload,
        getDelayMs: attempt => (attempt + 1) * 1000,
        ...retryOptions,
    });
}

async function gcpCreateMultipartUploadWithRetry(gcpClient, params, retryOptions) {
    const callFn = () => new Promise((resolve, reject) => {
        gcpClient.createMultipartUpload(params,
            (err, res) => (err ? reject(err) : resolve(res)));
    });
    return gcpRetryCall(callFn, {
        maxAttempts: 6,
        shouldRetry: defaultShouldRetryMpuCreate,
        getDelayMs: attempt => (attempt + 1) * 1000,
        ...retryOptions,
    });
}

// mpu test helpers
function gcpMpuSetup(params, callback) {
    const { gcpClient, bucketNames, key, partCount, partSize } = params;

    return async.waterfall([
        next => gcpCreateMultipartUploadWithRetry(gcpClient, {
                Bucket: bucketNames.mpu.Name,
                Key: key,
            })
                .then(res => next(null, res.UploadId))
                .catch(err => next(err)),
        (uploadId, next) => {
            if (partCount <= 0) {
                return next('SkipPutPart', { uploadId });
            }
            const arrayData = Array.from(Array(partCount).keys());
            const etagList = Array(partCount);
            let count = 0;
            return async.eachLimit(arrayData, 10,
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
                    if (!(++count % 100)) {
                        process.stdout.write(`Uploaded Parts: ${count}\n`);
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

function genPutTagObj(size, duplicate) {
    const retTagSet = [];
    Array.from(Array(size).keys()).forEach(ind => {
        retTagSet.push({
            Key: duplicate ? 'dupeKey' : `key${ind}`,
            Value: `Value${ind}`,
        });
    });
    return retTagSet;
}

function genGetTagObj(size, tagPrefix) {
    const retObj = {};
    const expectedTagObj = [];
    for (let i = 1; i <= size; ++i) {
        retObj[`${tagPrefix}testtag${i}`] = `testtag${i}`;
        expectedTagObj.push({
            Key: `testtag${i}`,
            Value: `testtag${i}`,
        });
    }
    return { tagHeader: retObj, expectedTagObj };
}

function genDelTagObj(size, tagPrefix) {
    const headers = {};
    const expectedTagObj = {};
    const expectedMetaObj = {};
    for (let i = 1; i <= size; ++i) {
        headers[`${tagPrefix}testtag${i}`] = `testtag${i}`;
        expectedTagObj[`${tagPrefix}testtag${i}`] = `testtag${i}`;
        headers[`x-goog-meta-testmeta${i}`] = `testmeta${i}`;
        expectedMetaObj[`x-goog-meta-testmeta${i}`] = `testmeta${i}`;
    }
    return { headers, expectedTagObj, expectedMetaObj };
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

async function waitForBucketReady(gcpClient, bucketName, retryOptions) {
    const cmd = new HeadBucketCommand({ Bucket: bucketName });
    return await gcpRetry(gcpClient, cmd, {
        maxAttempts: 6,
        shouldRetry: defaultShouldRetryMpuCreate,
        getDelayMs: attempt => (attempt + 1) * 1000,
        ...retryOptions,
    });
}

module.exports = {
    setBucketClass,
    gcpMpuSetup,
    genPutTagObj,
    genGetTagObj,
    genDelTagObj,
    genUniqID,
    gcpRetryCall,
    gcpRetry,
    gcpCreateMultipartUploadWithRetry,
    gcpUploadWithRetry,
    waitForBucketReady,
};
