const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const xml2js = require('xml2js');

const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../../lib/api/objectPutPart');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');

const { makeAuthInfo }
    = require('../helpers');

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);

// part 1
const partBody = Buffer.from('I am a part\n', 'utf8');
const md5Hash = crypto.createHash('md5').update(partBody);
const calculatedHash = md5Hash.digest('hex');

function createinitiateMPURequest(namespace, bucketName, objectKey) {
    const request = {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?uploads`,
        iamAuthzResults: false,
    };

    return request;
}

function createPutPartRequest(namespace, bucketName, objectKey, partNumber, testUploadId) {
    const request = new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: `/${objectKey}?partNumber=${partNumber}&uploadId=${testUploadId}`,
        query: {
            partNumber,
            uploadId: testUploadId,
        },
        calculatedHash,
        iamAuthzResults: false,
    }, partBody);

    return request;
}

function createCompleteRequest(namespace, bucketName, objectKey, testUploadId) {
  // only suports a single part for now
    const completeBody = '<CompleteMultipartUpload>' +
                         '<Part>' +
                         '<PartNumber>1</PartNumber>' +
                         `<ETag>"${calculatedHash}"</ETag>` +
                         '</Part>' +
                         '</CompleteMultipartUpload>';

    const request = {
        bucketName,
        namespace,
        objectKey,
        parsedHost: 's3.amazonaws.com',
        url: `/${objectKey}?uploadId=${testUploadId}`,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        query: { uploadId: testUploadId },
        post: completeBody,
        iamAuthzResults: false,
    };

    return request;
}

function createMPU(namespace, bucketName, objectKey, logger, cb) {
    let testUploadId;
    async.waterfall([
        next => {
            const initiateMPURequest = createinitiateMPURequest(namespace,
                                                                bucketName,
                                                                objectKey);
            initiateMultipartUpload(authInfo, initiateMPURequest, logger, next);
        },
        (result, corsHeaders, next) => xml2js.parseString(result, next),
        (json, next) => {
            testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
            const partRequest =
            createPutPartRequest(namespace, bucketName, objectKey, 1, testUploadId);
            objectPutPart(authInfo, partRequest, undefined, logger, next);
        },
        (hexDigest, corsHeaders, next) => {
            const completeRequest =
                createCompleteRequest(namespace, bucketName, objectKey, testUploadId);
            completeMultipartUpload(authInfo, completeRequest, logger, next);
        },
    ], (err) => {
        assert.ifError(err);
        cb(null, testUploadId);
    });

    return testUploadId;
}


module.exports = {
    createPutPartRequest,
    createCompleteRequest,
    createMPU,
};
