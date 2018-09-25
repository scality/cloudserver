const assert = require('assert');
const querystring = require('querystring');

const async = require('async');
const { parseString } = require('xml2js');

const bucketGet = require('../../../lib/api/bucketGet');
const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');

const { errors } = require('arsenal');


const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const delimiter = '/';
const log = new DummyRequestLogger();
const namespace = 'default';
const postBody = Buffer.from('I am a body', 'utf8');
const prefix = 'sub';

const objectName1 = `${prefix}${delimiter}objectName1`;
const objectName2 = `${prefix}${delimiter}objectName2`;
const objectName3 = 'notURIvalid$$';
const objectName4 = `${objectName1}&><"\'`;
const testPutBucketRequest = new DummyRequest({
    bucketName,
    headers: {},
    url: `/${bucketName}`,
    namespace,
}, Buffer.alloc(0));
const testPutObjectRequest1 = new DummyRequest({
    bucketName,
    headers: {},
    url: `/${bucketName}/${objectName1}`,
    namespace,
    objectKey: objectName1,
}, postBody);
const testPutObjectRequest2 = new DummyRequest({
    bucketName,
    headers: {},
    url: `/${bucketName}/${objectName2}`,
    namespace,
    objectKey: objectName2,
}, postBody);
const testPutObjectRequest3 = new DummyRequest({
    bucketName,
    headers: {},
    url: `/${bucketName}/${objectName3}`,
    namespace,
    objectKey: objectName3,
}, postBody);
const testPutObjectRequest4 = new DummyRequest({
    bucketName,
    headers: {},
    url: `/${bucketName}/${objectName3}`,
    namespace,
    objectKey: objectName4,
}, postBody);

const baseGetRequest = {
    bucketName,
    namespace,
    headers: { host: '/' },
};
const baseUrl = `/${bucketName}`;

const tests = [
    {
        name: 'list of all objects if no delimiter specified',
        request: Object.assign({ query: {}, url: baseUrl }, baseGetRequest),
        assertion: result => {
            assert.strictEqual(result.ListBucketResult.Contents[1].Key[0],
                objectName1);
            assert.strictEqual(result.ListBucketResult.Contents[2].Key[0],
                objectName2);
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                objectName3);
        },
    },
    {
        name: 'return name of common prefix of common prefix objects if ' +
            'delimiter and prefix specified',
        request: Object.assign({
            url: `/${bucketName}?delimiter=${delimiter}&prefix=${prefix}`,
            query: { delimiter, prefix },
        }, baseGetRequest),
        assertion: result =>
            assert.strictEqual(result.ListBucketResult
                .CommonPrefixes[0].Prefix[0], `${prefix}${delimiter}`),
    },
    {
        name: 'return empty list when max-keys is set to 0',
        request: Object.assign({ query: { 'max-keys': '0' }, url: baseUrl },
            baseGetRequest),
        assertion: result =>
            assert.strictEqual(result.ListBucketResult.Contents, undefined),
    },
    {
        name: 'return no more keys than max-keys specified',
        request: Object.assign({ query: { 'max-keys': '1' }, url: baseUrl },
            baseGetRequest),
        assertion: result => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                objectName3);
            assert.strictEqual(result.ListBucketResult.Contents[1], undefined);
        },
    },
    {
        name: 'return max-keys number from request even if greater than ' +
            'actual keys returned',
        request: Object.assign({ query: { 'max-keys': '99999' }, url: baseUrl },
            baseGetRequest),
        assertion: result =>
            assert.strictEqual(result.ListBucketResult.MaxKeys[0], '99999'),
    },
    {
        name: 'url encode object key name if requested',
        request: Object.assign(
            { query: { 'encoding-type': 'url' }, url: baseUrl },
            baseGetRequest),
        assertion: result => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                querystring.escape(objectName3));
            assert.strictEqual(result.ListBucketResult.Contents[1].Key[0],
                querystring.escape(objectName1));
        },
    },
];

describe('bucketGet API', () => {
    beforeEach(() => {
        cleanup();
    });

    tests.forEach(test => {
        it(`should ${test.name}`, done => {
            const testGetRequest = test.request;

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (corsHeaders, next) => objectPut(authInfo,
                        testPutObjectRequest1, undefined, log, next),
                (resHeaders, next) => objectPut(authInfo,
                        testPutObjectRequest2, undefined, log, next),
                (resHeaders, next) => objectPut(authInfo,
                    testPutObjectRequest3, undefined, log, next),
                (resHeaders, next) =>
                    bucketGet(authInfo, testGetRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
            ],
            (err, result) => {
                test.assertion(result);
                done();
            });
        });
    });

    it('should return an InvalidArgument error if max-keys == -1', done => {
        const testGetRequest = Object.assign({ query: { 'max-keys': '-1' } },
            baseGetRequest);
        bucketGet(authInfo, testGetRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    it('should escape invalid xml characters in object key names', done => {
        const testGetRequest = Object.assign({ query: {}, url: baseUrl },
            baseGetRequest);

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest4,
                undefined, log, next),
            (resHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                              testPutObjectRequest4.objectKey);
            done();
        });
    });

    it('should return xml that refers to the s3 docs for xml specs', done => {
        const testGetRequest = Object.assign({ query: {}, url: baseUrl },
            baseGetRequest);

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) =>
                bucketGet(authInfo, testGetRequest, log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.$.xmlns,
                'http://s3.amazonaws.com/doc/2006-03-01/');
            done();
        });
    });
});

describe('bucketGet API V2', () => {
    beforeEach(() => {
        cleanup();
    });

    tests.forEach(test => {
        /* eslint-disable no-param-reassign */
        test.request.query['list-type'] = 2;
        test.request.url = test.request.url.indexOf('?') > -1 ?
            `${test.request.url}&list-type=2` :
            `${test.request.url}?list-type=2`;
        /* eslint-enable no-param-reassign */

        it(`should return ${test.name}`, done => {
            const testGetRequest = test.request;

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (corsHeaders, next) => objectPut(authInfo,
                        testPutObjectRequest1, undefined, log, next),
                (resHeaders, next) => objectPut(authInfo,
                        testPutObjectRequest2, undefined, log, next),
                (resHeaders, next) => objectPut(authInfo,
                    testPutObjectRequest3, undefined, log, next),
                (resHeaders, next) =>
                    bucketGet(authInfo, testGetRequest, log, next),
                (result, corsHeaders, next) => parseString(result, next),
            ],
            (err, result) => {
                test.assertion(result);
                done();
            });
        });
    });
});
