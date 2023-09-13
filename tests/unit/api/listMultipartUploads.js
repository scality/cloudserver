const assert = require('assert');
const async = require('async');
const querystring = require('querystring');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const listMultipartUploads = require('../../../lib/api/listMultipartUploads');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
// TODO CLDSRV-431 remove skip
describe.skip('listMultipartUploads API', () => {
    beforeEach(() => {
        cleanup();
    });

    const prefix = 'sub';
    const delimiter = '/';
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;
    const objectName3 = 'notURIvalid$$';

    const testPutBucketRequest = {
        bucketName,
        namespace,
        headers: {},
        url: `/${bucketName}`,
        iamAuthzResults: false,
    };
    const testInitiateMPURequest1 = {
        bucketName,
        namespace,
        objectKey: objectName1,
        headers: {},
        url: `/${bucketName}/${objectName1}?uploads`,
        iamAuthzResults: false,
    };
    const testInitiateMPURequest2 = {
        bucketName,
        namespace,
        objectKey: objectName2,
        headers: {},
        url: `/${bucketName}/${objectName2}?uploads`,
        iamAuthzResults: false,
    };
    const testInitiateMPURequest3 = {
        bucketName,
        namespace,
        objectKey: objectName3,
        headers: {},
        url: `/${bucketName}/${objectName3}?uploads`,
        iamAuthzResults: false,
    };

    it('should return the name of the common prefix ' +
       'of common prefix object keys for multipart uploads if delimiter ' +
       'and prefix specified', done => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testListRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?uploads&delimiter=/&prefix=sub`,
            query: { delimiter, prefix },
            iamAuthzResults: false,
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest1, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest2, log, next),
            (result, corsHeaders, next) => listMultipartUploads(authInfo,
                testListRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, corsHeaders, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListMultipartUploadsResult
                .CommonPrefixes[0].Prefix[0],
                commonPrefix);
            done();
        });
    });

    it('should return list of all multipart uploads if ' +
       'no delimiter specified', done => {
        const testListRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?uploads`,
            query: {},
            iamAuthzResults: false,
        };


        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest1, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest2, log, next),
            (result, corsHeaders, next) =>
                listMultipartUploads(authInfo, testListRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, corsHeaders, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], objectName1);
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1].Key[0], objectName2);
            assert.strictEqual(result.ListMultipartUploadsResult
                .IsTruncated[0], 'false');
            done();
        });
    });

    it('should return no more keys than ' +
       'max-uploads specified', done => {
        const testListRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?uploads`,
            query: { 'max-uploads': '1' },
            iamAuthzResults: false,
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest1, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest2, log, next),
            (result, corsHeaders, next) => listMultipartUploads(authInfo,
                testListRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, corsHeaders, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], objectName1);
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1], undefined);
            assert.strictEqual(result.ListMultipartUploadsResult
                .IsTruncated[0], 'true');
            assert.strictEqual(result.ListMultipartUploadsResult
                .NextKeyMarker[0], objectName1);
            assert(result.ListMultipartUploadsResult
                .NextUploadIdMarker[0].length > 5);
            done();
        });
    });

    it('should url encode object key name ' +
       'if requested', done => {
        const testListRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?uploads`,
            query: { 'encoding-type': 'url' },
            iamAuthzResults: false,
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest1, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest2, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest3, log, next),
            (result, corsHeaders, next) => listMultipartUploads(authInfo,
                testListRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, corsHeaders, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], querystring.escape(objectName3));
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1].Key[0], querystring.escape(objectName1));
            done();
        });
    });

    it('should return key following specified ' +
    'key-marker', done => {
        const testListRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?uploads`,
            query: { 'key-marker': objectName1 },
            iamAuthzResults: false,
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest1, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest2, log, next),
            (result, corsHeaders, next) => initiateMultipartUpload(authInfo,
                testInitiateMPURequest3, log, next),
            (result, corsHeaders, next) => listMultipartUploads(authInfo,
                testListRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, corsHeaders, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], objectName2);
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1], undefined);
            done();
        });
    });
});
