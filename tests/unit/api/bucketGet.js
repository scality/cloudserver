import assert from 'assert';
import querystring from 'querystring';

import async from 'async';
import { parseString } from 'xml2js';

import bucketGet from '../../../lib/api/bucketGet';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';
import DummyRequest from '../DummyRequest';

import { errors } from 'arsenal';

const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const delimiter = '/';
const log = new DummyRequestLogger();
const namespace = 'default';
const postBody = Buffer.from('I am a body', 'utf8');
const prefix = 'sub';

let testPutBucketRequest;
let testPutObjectRequest1;
let testPutObjectRequest2;
let testPutObjectRequest3;

describe('bucketGet API', () => {
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;
    const objectName3 = 'notURIvalid$$';
    const post = '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<LocationConstraint>us-east-1</LocationConstraint>' +
        '</CreateBucketConfiguration>';

    beforeEach(() => {
        cleanup();
        testPutBucketRequest = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}`,
            namespace,
        }, post);
        testPutObjectRequest1 = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}/${objectName1}`,
            namespace,
            objectKey: objectName1,
        }, postBody);
        testPutObjectRequest2 = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}/${objectName2}`,
            namespace,
            objectKey: objectName2,
        }, postBody);
        testPutObjectRequest3 = new DummyRequest({
            bucketName,
            headers: {},
            url: `/${bucketName}/${objectName3}`,
            namespace,
            objectKey: objectName3,
        }, postBody);
    });

    it('should return the name of the common prefix of common prefix objects if'
       + 'delimiter and prefix specified', done => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}?delimiter=${delimiter}&prefix=${prefix}`,
            query: { delimiter, prefix },
        };

        async.waterfall([
            next =>
                bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) =>
                objectPut(authInfo, testPutObjectRequest1, undefined, log,
                    next),
            (result, corsHeaders, next) => {
                objectPut(authInfo, testPutObjectRequest2, undefined, log,
                    next);
            },
            (result, corsHeaders, next) =>
                bucketGet(authInfo, testGetRequest, log, next),
            (result, corsHeaders, next) =>
                parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult
                               .CommonPrefixes[0].Prefix[0],
                               commonPrefix);
            done();
        });
    });

    it('should return list of all objects if no delimiter specified', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: {},
        };


        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest1,
                undefined, log, next),
            (result, corsHeaders, next) => objectPut(authInfo,
                testPutObjectRequest2, undefined, log, next),
            (result, corsHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                               objectName1);
            assert.strictEqual(result.ListBucketResult.Contents[1].Key[0],
                               objectName2);
            done();
        });
    });

    it('should return no more keys than max-keys specified', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: { 'max-keys': '1' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest1,
                undefined, log, next),
            (result, corsHeaders, next) => objectPut(authInfo,
                testPutObjectRequest2, undefined, log, next),
            (result, corsHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                               objectName1);
            assert.strictEqual(result.ListBucketResult.Contents[1], undefined);
            done();
        });
    });

    it('should return an InvalidArgument error if max-keys == -1', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: { 'max-keys': '-1' },
        };
        bucketGet(authInfo, testGetRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    it('should return max-keys number from request even if greater than ' +
        'actual keys returned', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: { 'max-keys': '99999' },
        };
        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest1,
                undefined, log, next),
            (result, corsHeaders, next) => objectPut(authInfo,
                testPutObjectRequest2, undefined, log, next),
            (result, corsHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.MaxKeys[0],
                               '99999');
            done();
        });
    });

    it('should url encode object key name if requested', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: { 'encoding-type': 'url' },
        };

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest1,
                undefined, log, next),
            (result, corsHeaders, next) => objectPut(authInfo,
                testPutObjectRequest2, undefined, log, next),
            (result, corsHeaders, next) => objectPut(authInfo,
                testPutObjectRequest3, undefined, log, next),
            (result, corsHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                               querystring.escape(objectName3));
            assert.strictEqual(result.ListBucketResult.Contents[1].Key[0],
                               querystring.escape(objectName1));
            done();
        });
    });

    it('should escape invalid xml characters in object key names', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: {},
        };

        testPutObjectRequest1.objectKey += '&><"\'';
        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (corsHeaders, next) => objectPut(authInfo, testPutObjectRequest1,
                undefined, log, next),
            (result, corsHeaders, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, corsHeaders, next) => parseString(result, next),
        ],
        (err, result) => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                              testPutObjectRequest1.objectKey);
            done();
        });
    });

    it('should return xml that refers to the s3 docs for xml specs', done => {
        const testGetRequest = {
            bucketName,
            namespace,
            headers: { host: '/' },
            url: `/${bucketName}`,
            query: {},
        };

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
