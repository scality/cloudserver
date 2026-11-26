const assert = require('assert');
const querystring = require('querystring');

const async = require('async');
const { parseString } = require('xml2js');

const { bucketGet } = require('../../../lib/api/bucketGet');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { objectDelete } = require('../../../lib/api/objectDelete');
const objectPut = require('../../../lib/api/objectPut');
const metadata = require('../../../lib/metadata/wrapper');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');

const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const delimiter = '/';
const log = new DummyRequestLogger();
const namespace = 'default';
const postBody = Buffer.from('I am a body', 'utf8');
const prefix = 'sub';

const objectName1 = `${prefix}${delimiter}objectName1`;
const objectName2 = `${prefix}${delimiter}objectName2`;
const objectName3 = 'invalidURI~~~b';
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
    actionImplicitDenies: false,
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
        name: 'next token is not url encoded',
        request: Object.assign(
            {
                query: { 'max-keys': '1' },
                url: baseUrl,
            },
            baseGetRequest
        ),
        assertion: result => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                objectName3);
            assert.strictEqual(result.ListBucketResult.Contents[1], undefined);
            assert.strictEqual(
                result.ListBucketResult.NextContinuationToken[0],
                'aW52YWxpZFVSSX5+fmI='
            );
        },
    },
    {
        name: 'next token is not url encoded even with encoding url enabled',
        request: Object.assign(
            {
                query: { 'encoding-type': 'url', 'max-keys': '1' },
                url: baseUrl,
            },
            baseGetRequest
        ),
        assertion: result => {
            assert.strictEqual(result.ListBucketResult.Contents[0].Key[0],
                objectName3);
            assert.strictEqual(result.ListBucketResult.Contents[1], undefined);
            assert.strictEqual(
                result.ListBucketResult.NextContinuationToken[0],
                'aW52YWxpZFVSSX5+fmI='
            );
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
        name: 'return max-keys number from request even when value is 0',
        request: Object.assign({ query: { 'max-keys': '0' }, url: baseUrl },
            baseGetRequest),
        assertion: result =>
            assert.strictEqual(result.ListBucketResult.MaxKeys[0], '0'),
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
                (_, next) => objectPut(authInfo,
                        testPutObjectRequest1, undefined, log, next),
                (_, next) => objectPut(authInfo,
                        testPutObjectRequest2, undefined, log, next),
                (_, next) => objectPut(authInfo,
                    testPutObjectRequest3, undefined, log, next),
                (_, next) =>
                    bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (_, result) => {
                test.assertion(result);
                done();
            });
        });
    });

    it('should return an InvalidArgument error if max-keys == -1', done => {
        const testGetRequest = Object.assign({ query: { 'max-keys': '-1' } },
            baseGetRequest);
        bucketGet(authInfo, testGetRequest, log, err => {
            assert.strictEqual(err.is.InvalidArgument, true);
            done();
        });
    });

    it('should escape invalid xml characters in object key names', done => {
        const testGetRequest = Object.assign({ query: {}, url: baseUrl },
            baseGetRequest);

        async.waterfall([
            next => bucketPut(authInfo, testPutBucketRequest, log, next),
            (_, next) => objectPut(authInfo, testPutObjectRequest4,
                undefined, log, next),
            (_, next) => bucketGet(authInfo, testGetRequest,
                log, next),
            (result, _, next) => parseString(result, next),
        ],
        (_, result) => {
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
            (_, next) =>
                bucketGet(authInfo, testGetRequest, log, next),
            (result, _, next) => parseString(result, next),
        ],
        (_, result) => {
            assert.strictEqual(result.ListBucketResult.$.xmlns,
                'http://s3.amazonaws.com/doc/2006-03-01/');
            done();
        });
    });
});

const testsForV2 = [...tests,
    {
        name: 'return no owner info when --fetch-owner option is not used',
        request: Object.assign({ query: {}, url: baseUrl }, baseGetRequest),
        assertion: result => {
            const owners
                = result.ListBucketResult.Contents.filter(c => c.Owner);
            assert.strictEqual(owners.length, 0);
        },
    },
    {
        name: 'return owner info when --fetch-owner option is used',
        request: Object.assign({ query: { 'fetch-owner': 'true' },
            url: baseUrl }, baseGetRequest),
        assertion: result => {
            const owners
                = result.ListBucketResult.Contents.filter(c =>
                    c.Owner[0].ID[0] === authInfo.canonicalID &&
                    c.Owner[0].DisplayName[0] === authInfo.accountDisplayName);
            assert.strictEqual(owners.length,
                result.ListBucketResult.Contents.length);
        },
    },
    {
        name: 'return no owner info when --no-fetch-owner option is used',
        request: Object.assign({ query: { 'fetch-owner': 'false' },
            url: baseUrl }, baseGetRequest),
        assertion: result => {
            const owners
                = result.ListBucketResult.Contents.filter(c => c.Owner);
            assert.strictEqual(owners.length, 0);
        },
    },
    {
        name: 'return max-keys number from request even when value is 0',
        request: Object.assign({ query: { 'max-keys': '0' }, url: baseUrl },
            baseGetRequest),
        assertion: result =>
            assert.strictEqual(result.ListBucketResult.MaxKeys[0], '0'),
    },
];

describe('bucketGet API V2', () => {
    beforeEach(() => {
        cleanup();
    });

    testsForV2.forEach(test => {
        /* eslint-disable no-param-reassign */
        test.request.query['list-type'] = 2;
        test.request.url = test.request.url.indexOf('?') > -1 ?
            `${test.request.url}&list-type=2` :
            `${test.request.url}?list-type=2`;
        /* eslint-enable no-param-reassign */

        it(`should ${test.name}`, done => {
            const testGetRequest = test.request;

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo,
                        testPutObjectRequest1, undefined, log, next),
                (_, next) => objectPut(authInfo,
                        testPutObjectRequest2, undefined, log, next),
                (_, next) => objectPut(authInfo,
                    testPutObjectRequest3, undefined, log, next),
                (_, next) =>
                    bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (_, result) => {
                // v2 requests should return 'KeyCount' in response
                const keyCount =
                    Number.parseInt(result.ListBucketResult.KeyCount[0], 10);
                const keysReturned = result.ListBucketResult.Contents ?
                    result.ListBucketResult.Contents.length : 0;
                assert.strictEqual(keyCount, keysReturned);
                // assert the results from tests
                test.assertion(result);
                if (result.ListBucketResult.IsTruncated && result.ListBucketResult.IsTruncated[0] === 'false') {
                    assert.strictEqual(result.ListBucketResult.NextContinuationToken, undefined);
                }
                done();
            });
        });
    });

    describe('x-amz-optional-object-attributes header', () => {
        const archiveInfo = { archiveId: 'archive-1', archiveVersion: 1 };
        const hour = 60 * 60 * 1000;
        const day = 24 * hour;

        const inProgressArchive = () => ({
            archiveInfo,
            restoreRequestedAt: new Date().toISOString(),
            restoreRequestedDays: 2,
        });

        const restoredArchive = () => {
            const now = Date.now();
            return {
                archiveInfo,
                restoreRequestedAt: new Date(now - day).toISOString(),
                restoreRequestedDays: 2,
                restoreCompletedAt: new Date(now - day + hour).toISOString(),
                restoreWillExpireAt: new Date(now + day).toISOString(),
            };
        };

        // sets the archive field of an object that was put, as the tests of objectGet do
        const setArchive = (key, archive, cb) => {
            metadata.getObjectMD(bucketName, key, {}, log, (err, objMD) => {
                if (err) {
                    return cb(err);
                }
                // eslint-disable-next-line no-param-reassign
                objMD.archive = archive;
                return metadata.putObjectMD(bucketName, key, objMD, { versionId: objMD.versionId }, log,
                    putErr => cb(putErr));
            });
        };

        // puts an object and, when `archive` is given, sets its archive field
        const putObjectInState = (key, archive, cb) => {
            const putRequest = new DummyRequest({
                bucketName,
                headers: {},
                url: `/${bucketName}/${key}`,
                namespace,
                objectKey: key,
            }, postBody);
            objectPut(authInfo, putRequest, undefined, log, putErr => {
                if (putErr || !archive) {
                    return cb(putErr);
                }
                return setArchive(key, archive, cb);
            });
        };

        const listWithHeader = (query, headerValue, cb) => {
            const request = Object.assign({
                query,
                url: `${baseUrl}?${querystring.stringify(query)}`,
            }, baseGetRequest);
            request.headers = Object.assign({}, baseGetRequest.headers);
            if (headerValue !== undefined) {
                request.headers['x-amz-optional-object-attributes'] = headerValue;
            }
            bucketGet(authInfo, request, log, (err, result) => {
                if (err) {
                    return cb(err);
                }
                return parseString(result, cb);
            });
        };

        it('should return an error if the header is empty', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = '';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ], err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });

        it('should return an error for invalid optional attributes', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'InvalidAttribute';

            async.series([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                next =>  bucketGet(authInfo, testGetRequest, log, next),
            ], err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });

        it('should accept wildcard value', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-*';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ], (err, result) => {
                assert.strictEqual(err, null);
                assert.strictEqual(result.ListBucketResult.$.xmlns, 'http://s3.amazonaws.com/doc/2006-03-01/');
                done();
            });
        });

        it('should return an error for a mix of valid and invalid attributes', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'RestoreStatus,InvalidAttribute';

            async.series([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                next =>  bucketGet(authInfo, testGetRequest, log, next),
            ], err => {
                assert.strictEqual(err.is.InvalidArgument, true);
                done();
            });
        });

        it('should handle attributes with leading/trailing whitespace', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = '   x-amz-meta-foo    ';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                assert.strictEqual(result.ListBucketResult.$.xmlns, 'http://s3.amazonaws.com/doc/2006-03-01/');
                done();
            });
        });

        it('should handle multiple valid attributes', done => {
            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'RestoreStatus,x-amz-meta-foo,x-amz-meta-bar';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                assert.strictEqual(result.ListBucketResult.$.xmlns, 'http://s3.amazonaws.com/doc/2006-03-01/');
                done();
            });
        });

        it('should return user metadata if requested and present', done => {
            const objectNameMeta = 'objectWithMeta';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-color': 'red' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);

            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-color';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const content = result.ListBucketResult.Contents[0];
                assert.strictEqual(content.Key[0], objectNameMeta);
                assert.strictEqual(content['x-amz-meta-color'][0], 'red');
                done();
            });
        });

        it('should not duplicate elements when the header repeats tokens', done => {
            const objectNameMeta = 'objectWithRepeatedTokens';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-color': 'red' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);

            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] =
                'RestoreStatus,RestoreStatus,x-amz-meta-color,x-amz-meta-color';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => setArchive(objectNameMeta, restoredArchive(), next),
                next => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const content = result.ListBucketResult.Contents[0];
                assert.strictEqual(content.Key[0], objectNameMeta);
                assert.strictEqual(content.RestoreStatus.length, 1);
                assert.strictEqual(content['x-amz-meta-color'].length, 1);
                assert.strictEqual(content['x-amz-meta-color'][0], 'red');
                done();
            });
        });

        it('should return all user metadata if wildcard requested', done => {
            const objectNameMeta = 'objectWithMetaWildcard';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-color': 'red', 'x-amz-meta-size': 'large' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);

            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-*';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const content = result.ListBucketResult.Contents[0];
                assert.strictEqual(content.Key[0], objectNameMeta);
                assert.strictEqual(content['x-amz-meta-color'][0], 'red');
                assert.strictEqual(content['x-amz-meta-size'][0], 'large');
                done();
            });
        });

        it('should return user metadata in version listing if requested', done => {
            const objectNameMeta = 'objectWithMetaVersion';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-ver': '1' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);

            const testGetRequest = Object.assign({
                query: { versions: '' },
                url: `${baseUrl}?versions`,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-ver';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const version = result.ListVersionsResult.Version[0];
                assert.strictEqual(version.Key[0], objectNameMeta);
                assert.strictEqual(version['x-amz-meta-ver'][0], '1');
                done();
            });
        });

        it('should not include optional attributes on delete markers in versions listing', done => {
            const objectNameMeta = 'objectWithMetaAndDeleteMarker';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-color': 'red' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);
            const deleteRequest = new DummyRequest({
                bucketName,
                headers: {},
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
                actionImplicitDenies: false,
            });
            const versioningRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                url: '/?versioning',
                query: { versioning: '' },
                actionImplicitDenies: false,
                post: '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    + '<Status>Enabled</Status></VersioningConfiguration>',
            };

            const testGetRequest = Object.assign({
                query: { versions: '' },
                url: `${baseUrl}?versions`,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] =
                'RestoreStatus,x-amz-meta-color';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => bucketPutVersioning(authInfo, versioningRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => setArchive(objectNameMeta, restoredArchive(), next),
                next => objectDelete(authInfo, deleteRequest, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const { Version, DeleteMarker } = result.ListVersionsResult;
                assert.strictEqual(Version.length, 1);
                assert.strictEqual(Version[0].Key[0], objectNameMeta);
                assert.strictEqual(Version[0]['x-amz-meta-color'][0], 'red');
                assert.strictEqual(Version[0].RestoreStatus.length, 1);

                assert.strictEqual(DeleteMarker.length, 1);
                assert.strictEqual(DeleteMarker[0].Key[0], objectNameMeta);
                assert.strictEqual(DeleteMarker[0]['x-amz-meta-color'], undefined);
                assert.strictEqual(DeleteMarker[0].RestoreStatus, undefined);
                done();
            });
        });

        it('should return user metadata as case insentive (lowercase header)', done => {
            const objectNameMeta = 'objectWithMeta';
            const putRequest = new DummyRequest({
                bucketName,
                headers: { 'x-amz-meta-color': 'yellow' },
                url: `/${bucketName}/${objectNameMeta}`,
                namespace,
                objectKey: objectNameMeta,
            }, postBody);

            const testGetRequest = Object.assign({
                query: {},
                url: baseUrl,
            }, baseGetRequest);
            testGetRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-coLor';

            async.waterfall([
                next => bucketPut(authInfo, testPutBucketRequest, log, next),
                (_, next) => objectPut(authInfo, putRequest, undefined, log, next),
                (_, next) => bucketGet(authInfo, testGetRequest, log, next),
                (result, _, next) => parseString(result, next),
            ],
            (err, result) => {
                assert.strictEqual(err, null);
                const content = result.ListBucketResult.Contents[0];
                assert.strictEqual(content.Key[0], objectNameMeta);
                assert.strictEqual(content['x-amz-meta-color'][0], 'yellow');
                done();
            });
        });

        describe('RestoreStatus', () => {
            const states = [
                { name: 'never-archived', archive: () => undefined },
                { name: 'archived-never-restored', archive: () => ({ archiveInfo }) },
                { name: 'restore-in-progress', archive: inProgressArchive },
                { name: 'restored', archive: restoredArchive },
            ];
            // the elements each key of the four states above must carry, by key
            const expectRestoreStatusByState = (entries, expectedExpiry) => {
                const byKey = {};
                entries.forEach(entry => { byKey[entry.Key[0]] = entry; });
                assert.strictEqual(Object.keys(byKey).length, 4);
                assert.strictEqual(byKey['never-archived'].RestoreStatus, undefined);
                assert.strictEqual(byKey['archived-never-restored'].RestoreStatus, undefined);
                assert.deepStrictEqual(byKey['restore-in-progress'].RestoreStatus,
                    [{ IsRestoreInProgress: ['true'] }]);
                assert.deepStrictEqual(byKey.restored.RestoreStatus,
                    [{ IsRestoreInProgress: ['false'], RestoreExpiryDate: [expectedExpiry] }]);
            };
            const putAllStates = cb => async.eachSeries(states, (state, next) => {
                const archive = state.archive();
                if (archive && archive.restoreWillExpireAt) {
                    archive.restoreWillExpireAt = expiry;
                }
                putObjectInState(state.name, archive, next);
            }, cb);
            const expiry = new Date(Date.now() + day).toISOString();

            it('should not return RestoreStatus for an object that was never archived', done => {
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putObjectInState('plain', undefined, next),
                    next => listWithHeader({ 'list-type': 2 }, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    const { Contents } = result.ListBucketResult;
                    assert.strictEqual(Contents.length, 1);
                    assert.strictEqual(Contents[0].Key[0], 'plain');
                    assert.strictEqual(Contents[0].RestoreStatus, undefined);
                    done();
                });
            });

            it('should not return RestoreStatus for an archived object that was never restored', done => {
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putObjectInState('archived', { archiveInfo }, next),
                    next => listWithHeader({ 'list-type': 2 }, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    const { Contents } = result.ListBucketResult;
                    assert.strictEqual(Contents.length, 1);
                    assert.strictEqual(Contents[0].Key[0], 'archived');
                    assert.strictEqual(Contents[0].RestoreStatus, undefined);
                    done();
                });
            });

            it('should return IsRestoreInProgress=true and no RestoreExpiryDate while a restore is in progress',
            done => {
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putObjectInState('restoring', inProgressArchive(), next),
                    next => listWithHeader({ 'list-type': 2 }, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    const { Contents } = result.ListBucketResult;
                    assert.strictEqual(Contents.length, 1);
                    assert.deepStrictEqual(Contents[0].RestoreStatus, [{ IsRestoreInProgress: ['true'] }]);
                    done();
                });
            });

            it('should return IsRestoreInProgress=false and RestoreExpiryDate for a restored object', done => {
                const archive = restoredArchive();
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putObjectInState('restored', archive, next),
                    next => listWithHeader({ 'list-type': 2 }, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    const { Contents } = result.ListBucketResult;
                    assert.strictEqual(Contents.length, 1);
                    assert.deepStrictEqual(Contents[0].RestoreStatus, [{
                        IsRestoreInProgress: ['false'],
                        RestoreExpiryDate: [archive.restoreWillExpireAt],
                    }]);
                    done();
                });
            });

            it('should return RestoreStatus by restore state in ListObjects v1', done => {
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putAllStates(next),
                    next => listWithHeader({}, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    expectRestoreStatusByState(result.ListBucketResult.Contents, expiry);
                    done();
                });
            });

            it('should return RestoreStatus by the restore state of each version in versions listing', done => {
                const versioningRequest = {
                    bucketName,
                    headers: { host: `${bucketName}.s3.amazonaws.com` },
                    url: '/?versioning',
                    query: { versioning: '' },
                    actionImplicitDenies: false,
                    post: '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                        + '<Status>Enabled</Status></VersioningConfiguration>',
                };
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => bucketPutVersioning(authInfo, versioningRequest, log, next),
                    (_, next) => putAllStates(next),
                    next => listWithHeader({ versions: '' }, 'RestoreStatus', next),
                ], (err, result) => {
                    assert.ifError(err);
                    expectRestoreStatusByState(result.ListVersionsResult.Version, expiry);
                    done();
                });
            });

            it('should not return RestoreStatus when the header is absent, whatever the restore state', done => {
                async.waterfall([
                    next => bucketPut(authInfo, testPutBucketRequest, log, next),
                    (_, next) => putAllStates(next),
                    next => listWithHeader({ 'list-type': 2 }, undefined, next),
                ], (err, result) => {
                    assert.ifError(err);
                    const { Contents } = result.ListBucketResult;
                    assert.strictEqual(Contents.length, 4);
                    Contents.forEach(entry => assert.strictEqual(entry.RestoreStatus, undefined));
                    done();
                });
            });
        });
    });
});
