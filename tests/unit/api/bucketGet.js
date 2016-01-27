import assert from 'assert';
import async from 'async';

import { parseString } from 'xml2js';

import bucketGet from '../../../lib/api/bucketGet';
import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import objectPut from '../../../lib/api/objectPut';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = [ new Buffer('I am a body'), ];

describe('bucketGet API', () => {
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, () => done());
    });


    const prefix = 'sub';
    const delimiter = '/';
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;
    const objectName3 = 'notURIvalid$$';

    const testPutBucketRequest = {
        bucketName,
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace,
    };
    const testPutObjectRequest1 = {
        bucketName,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName1}`,
        namespace,
        post: postBody,
        objectKey: objectName1,
    };
    const testPutObjectRequest2 = {
        bucketName,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName2}`,
        namespace,
        post: postBody,
        objectKey: objectName2,
    };
    const testPutObjectRequest3 = {
        bucketName,
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName3}`,
        namespace,
        post: postBody,
        objectKey: objectName3,
    };

    it('should return the name of the common prefix ' +
       'of common prefix objects if delimiter ' +
       'and prefix specified', (done) => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testGetRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?delimiter=/&prefix=sub`,
            query: {
                delimiter: delimiter,
                prefix: prefix
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,  testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,  testPutObjectRequest1, log,
                    next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey,  testPutObjectRequest2, log,
                    next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey,  testGetRequest, log,
                    next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListBucketResult
                .CommonPrefixes[0].Prefix[0], commonPrefix);
            done();
        });
    });

    it('should return list of all objects if ' +
       'no delimiter specified', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            query: {}
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,  testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,  testPutObjectRequest1, log,
                    next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey,  testPutObjectRequest2, log,
                    next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey,  testGetRequest, log, next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListBucketResult
                .Contents[0].Key[0], objectName1);
            assert.strictEqual(result.ListBucketResult
                .Contents[1].Key[0], objectName2);
            done();
        });
    });

    it('should return no more keys than ' +
       'max-keys specified', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            query: {
                'max-keys': '1',
            }
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,  testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,  testPutObjectRequest1, log,
                    next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey,  testPutObjectRequest2, log,
                    next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey,  testGetRequest, log,
                    next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListBucketResult
                .Contents[0].Key[0], objectName1);
            assert.strictEqual(result.ListBucketResult.Contents[1], undefined);
            done();
        });
    });

    it('should url encode object key name ' +
       'if requested', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            query: {
                'encoding-type': 'url',
            }
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,  testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,  testPutObjectRequest1, log,
                    next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey,  testPutObjectRequest2, log,
                    next);
            },
            function waterfall4(result, next) {
                objectPut(accessKey,  testPutObjectRequest3, log,
                    next);
            },
            function waterfall5(result, next) {
                bucketGet(accessKey,  testGetRequest, log, next);
            },
            function waterfall6(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListBucketResult
                .Contents[0].Key[0], encodeURIComponent(objectName3));
            assert.strictEqual(result.ListBucketResult
                .Contents[1].Key[0], encodeURIComponent(objectName1));
            done();
        });
    });

    it('should return the correct date in the xml attributes', (done) => {
        const testGetRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?`,
            query: {}
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,
                    testPutBucketRequest, log, next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                objectPut(accessKey,
                    testPutObjectRequest1, log, next);
            },
            function waterfall3(result, next) {
                bucketGet(accessKey,
                    testGetRequest, log, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            const dateNow = new Date();
            let month = (dateNow.getMonth() + 1).toString();
            if (month.length === 1) {
                month = `0${month}`;
            }
            const dateString =
                `${dateNow.getFullYear()}-${month}-${dateNow.getDate()}`;
            const resultDate = result.ListBucketResult.$.xmlns.slice(-10);
            assert.strictEqual(dateString, resultDate);
            assert.strictEqual(resultDate.indexOf('NaN'), -1);
            done();
        });
    });
});
