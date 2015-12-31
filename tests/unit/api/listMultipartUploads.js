import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import bucketPut from '../../../lib/api/bucketPut';
import initiateMultipartUpload from '../../../lib/api/initiateMultipartUpload';
import listMultipartUploads from '../../../lib/api/listMultipartUploads';
import metadata from '../metadataswitch';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();

const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName = 'bucketname';
const mpuBucket = `mpu...${bucketName}`;

describe('listMultipartUploads API', () => {
    let metastore;

    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {}
        };

        // Must delete real bucket and shadow mpu bucket
        metadata.deleteBucket(bucketName, () => {
            metadata.deleteBucket(mpuBucket, () => {
                done();
            });
        });
    });

    after((done) => {
        metadata.deleteBucket(bucketName, () => {
            metadata.deleteBucket(mpuBucket, () => {
                done();
            });
        });
    });

    const prefix = 'sub';
    const delimiter = '/';
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;
    const objectName3 = 'notURIvalid$$';

    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace,
    };
    const testInitiateMPURequest1 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName1}?uploads`,
        namespace,
    };
    const testInitiateMPURequest2 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName2}?uploads`,
        namespace,
    };
    const testInitiateMPURequest3 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName3}?uploads`,
        namespace,
    };

    it('should return the name of the common prefix ' +
       'of common prefix object keys for multipart uploads if delimiter ' +
       'and prefix specified', (done) => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testListRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?uploads&delimiter=/&prefix=sub`,
            namespace,
            query: {
                delimiter: delimiter,
                prefix: prefix
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest1, log, next);
            },
            function waterfall3(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest2, log, next);
            },
            function waterfall4(result, next) {
                listMultipartUploads(accessKey, metastore,
                    testListRequest, log, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListMultipartUploadsResult
                .CommonPrefixes[0].Prefix[0],
                commonPrefix);
            done();
        });
    });

    it('should return list of all multipart uploads if ' +
       'no delimiter specified', (done) => {
        const testListRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?uploads`,
            namespace,
            query: {}
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest1, log, next);
            },
            function waterfall3(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest2, log, next);
            },
            function waterfall4(result, next) {
                listMultipartUploads(accessKey, metastore,
                    testListRequest, log, next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
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
       'max-uploads specified', (done) => {
        const testListRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?uploads`,
            namespace,
            query: {
                'max-uploads': '1',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest1, log, next);
            },
            function waterfall3(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest2, log, next);
            },
            function waterfall4(result, next) {
                listMultipartUploads(accessKey, metastore,
                    testListRequest, log, next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], objectName1);
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1], undefined);
            assert.strictEqual(result.ListMultipartUploadsResult
                .IsTruncated[0], 'true');
            assert.strictEqual(result.ListMultipartUploadsResult
                .NextKeyMarker[0], objectName2);
            assert(result.ListMultipartUploadsResult
                .NextUploadIdMarker[0].length > 5);
            done();
        });
    });

    it('should url encode object key name ' +
       'if requested', (done) => {
        const testListRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?uploads`,
            namespace,
            query: {
                'encoding-type': 'url',
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest1, log, next);
            },
            function waterfall3(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest2, log, next);
            },
            function waterfall4(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest3, log, next);
            },
            function waterfall5(result, next) {
                listMultipartUploads(accessKey, metastore,
                    testListRequest, log, next);
            },
            function waterfall6(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], encodeURIComponent(objectName3));
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1].Key[0], encodeURIComponent(objectName1));
            done();
        });
    });

    it('should return key following specified ' +
    'key-marker', (done) => {
        const testListRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?uploads`,
            namespace,
            query: {
                'key-marker': objectName1,
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, log,
                    next);
            },
            function waterfall2(success, next) {
                assert.strictEqual(success, 'Bucket created');
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest1, log, next);
            },
            function waterfall3(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest2, log, next);
            },
            function waterfall4(result, next) {
                initiateMultipartUpload(accessKey, metastore,
                    testInitiateMPURequest3, log, next);
            },
            function waterfall5(result, next) {
                listMultipartUploads(accessKey, metastore,
                    testListRequest, log, next);
            },
            function waterfall6(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[0].Key[0], objectName2);
            assert.strictEqual(result.ListMultipartUploadsResult
                .Upload[1], undefined);
            done();
        });
    });
});
