import { expect } from 'chai';
import async from 'async';
import { parseString } from 'xml2js';
import bucketPut from '../../../lib/api/bucketPut';
import objectPut from '../../../lib/api/objectPut';
import bucketGet from '../../../lib/api/bucketGet';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('bucketGet API', () => {
    let metastore;
    let datastore;

    beforeEach(() => {
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
        datastore = {};
    });

    const bucketName = 'bucketname';
    const postBody = 'I am a body';
    const prefix = 'sub';
    const delimiter = '/';
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;
    const objectName3 = 'notURIvalid$$';

    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const testPutObjectRequest1 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName1}`,
        namespace: namespace,
        post: postBody,
    };
    const testPutObjectRequest2 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName2}`,
        namespace: namespace,
        post: postBody
    };
    const testPutObjectRequest3 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName3}`,
        namespace: namespace,
        post: postBody
    };

    it('should return the name of the common prefix ' +
       'of common prefix objects if delimiter ' +
       'and prefix specified', (done) => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?delimiter=\/&prefix=sub`,
            namespace: namespace,
            query: {
                delimiter: delimiter,
                prefix: prefix
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.CommonPrefixes[0].Prefix[0])
                .to.equal(commonPrefix);
            done();
        });
    });

    it('should return list of all objects if ' +
       'no delimiter specified', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            namespace: namespace,
            query: {}
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.Contents[0].Key[0])
                .to.equal(objectName1);
            expect(result.ListBucketResult.Contents[1].Key[0])
                .to.equal(objectName2);
            done();
        });
    });

    it('should return no more keys than ' +
       'max-keys specified', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            namespace: namespace,
            query: {
                'max-keys': '1',
            }
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall5(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.Contents[0].Key[0])
                .to.equal(objectName1);
            expect(result.ListBucketResult.Contents[1])
                .to.be.undefined;
            done();
        });
    });

    it('should url encode object key name ' +
       'if requested', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            namespace: namespace,
            query: {
                'encoding-type': 'url',
            }
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest3, next);
            },
            function waterfall5(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall6(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.Contents[0].Key[0])
                .to.equal(encodeURIComponent(objectName3));
            expect(result.ListBucketResult.Contents[1].Key[0])
                .to.equal(encodeURIComponent(objectName1));
            done();
        });
    });
});
