const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const xml2js = require('xml2js');

const DummyRequest = require('../DummyRequest');
const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload = require('../../../lib/api/initiateMultipartUpload');
const objectPutPart = require('../../../lib/api/objectPutPart');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const metadataswitch = require('../metadataswitch');

const { makeAuthInfo } = require('../helpers');

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);

// part 1
const partBody = Buffer.from('I am a part\n', 'utf8');
const md5Hash = crypto.createHash('md5').update(partBody);
const calculatedHash = md5Hash.digest('hex');

function createBucketPutRequest(namespace, bucketName, location = 'scality-internal-mem') {
    return {
        bucketName,
        namespace,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
        post:
            '<CreateBucketConfiguration ' +
            'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            `<LocationConstraint>${location}</LocationConstraint>` +
            '</CreateBucketConfiguration >',
        actionImplicitDenies: false,
    };
}

function createinitiateMPURequest(namespace, bucketName, objectKey, extraHeaders = {}) {
    const request = {
        bucketName,
        namespace,
        objectKey,
        headers: { host: `${bucketName}.s3.amazonaws.com`, ...extraHeaders },
        url: `/${objectKey}?uploads`,
        actionImplicitDenies: false,
    };

    return request;
}

function createPutPartRequest(namespace, bucketName, objectKey, partNumber, testUploadId, extraHeaders = {}) {
    const request = new DummyRequest(
        {
            bucketName,
            namespace,
            objectKey,
            headers: { host: `${bucketName}.s3.amazonaws.com`, ...extraHeaders },
            url: `/${objectKey}?partNumber=${partNumber}&uploadId=${testUploadId}`,
            query: {
                partNumber,
                uploadId: testUploadId,
            },
            calculatedHash,
            actionImplicitDenies: false,
        },
        partBody,
    );

    return request;
}

function createCompleteRequest(namespace, bucketName, objectKey, testUploadId, opts = {}) {
    const { extraHeaders = {}, partChecksumXml = '' } = opts;
    // only supports a single part for now
    const completeBody =
        '<CompleteMultipartUpload>' +
        '<Part>' +
        '<PartNumber>1</PartNumber>' +
        `<ETag>"${calculatedHash}"</ETag>${partChecksumXml}` +
        '</Part>' +
        '</CompleteMultipartUpload>';

    const request = {
        bucketName,
        namespace,
        objectKey,
        parsedHost: 's3.amazonaws.com',
        url: `/${objectKey}?uploadId=${testUploadId}`,
        headers: { host: `${bucketName}.s3.amazonaws.com`, ...extraHeaders },
        query: { uploadId: testUploadId },
        post: completeBody,
        actionImplicitDenies: false,
    };

    return request;
}

function createMPU(namespace, bucketName, objectKey, logger, cb) {
    let testUploadId;
    async.waterfall(
        [
            next => {
                const initiateMPURequest = createinitiateMPURequest(namespace, bucketName, objectKey);
                initiateMultipartUpload(authInfo, initiateMPURequest, logger, next);
            },
            (result, corsHeaders, next) => xml2js.parseString(result, next),
            (json, next) => {
                testUploadId = json.InitiateMultipartUploadResult.UploadId[0];
                const partRequest = createPutPartRequest(namespace, bucketName, objectKey, 1, testUploadId);
                objectPutPart(authInfo, partRequest, undefined, logger, next);
            },
            (hexDigest, corsHeaders, next) => {
                const completeRequest = createCompleteRequest(namespace, bucketName, objectKey, testUploadId);
                completeMultipartUpload(authInfo, completeRequest, logger, next);
            },
        ],
        err => {
            assert.ifError(err);
            cb(null, testUploadId);
        },
    );

    return testUploadId;
}

function bucketPutP(bucketName, namespace, logger, location) {
    return new Promise((resolve, reject) => {
        bucketPut(authInfo, createBucketPutRequest(namespace, bucketName, location), logger, err =>
            err ? reject(err) : resolve(),
        );
    });
}

function initiateMpuP(bucketName, namespace, objectKey, logger, extraHeaders) {
    return new Promise((resolve, reject) => {
        const req = createinitiateMPURequest(namespace, bucketName, objectKey, extraHeaders);
        initiateMultipartUpload(authInfo, req, logger, (err, xml) => {
            if (err) {
                return reject(err);
            }
            return xml2js.parseString(xml, (parseErr, json) =>
                parseErr ? reject(parseErr) : resolve(json.InitiateMultipartUploadResult.UploadId[0]),
            );
        });
    });
}

function uploadPartP(bucketName, namespace, objectKey, uploadId, logger, extraHeaders) {
    return new Promise((resolve, reject) => {
        const req = createPutPartRequest(namespace, bucketName, objectKey, 1, uploadId, extraHeaders);
        objectPutPart(authInfo, req, undefined, logger, err => (err ? reject(err) : resolve()));
    });
}

// Resolves with { xml, headers } so callers can inspect the response body and
// headers (e.g. to assert which checksum-related fields the response carries).
function completeMpuP(bucketName, namespace, objectKey, uploadId, logger, opts) {
    return new Promise((resolve, reject) => {
        const req = createCompleteRequest(namespace, bucketName, objectKey, uploadId, opts);
        completeMultipartUpload(authInfo, req, logger, (err, xml, headers) =>
            err ? reject(err) : resolve({ xml, headers }),
        );
    });
}

function getObjectMDP(bucketName, objectKey, logger) {
    return new Promise((resolve, reject) =>
        metadataswitch.getObjectMD(bucketName, objectKey, {}, logger, (err, md) => (err ? reject(err) : resolve(md))),
    );
}

function parseXmlP(xmlStr) {
    return new Promise((resolve, reject) =>
        xml2js.parseString(xmlStr, (err, json) => (err ? reject(err) : resolve(json))),
    );
}

module.exports = {
    authInfo,
    partBody,
    calculatedHash,
    createBucketPutRequest,
    createinitiateMPURequest,
    createPutPartRequest,
    createCompleteRequest,
    createMPU,
    bucketPutP,
    initiateMpuP,
    uploadPartP,
    completeMpuP,
    getObjectMDP,
    parseXmlP,
};
