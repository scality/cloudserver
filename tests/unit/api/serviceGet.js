import { expect } from 'chai';
import { parseString } from 'xml2js';
import async from 'async';
import serviceGet from '../../../lib/api/serviceGet.js';
import bucketPut from '../../../lib/api/bucketPut.js';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('serviceGet API', () => {
    let metastore;

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
    });

    it('should return the list of buckets owned by the user', (done) => {
        const bucketName1 = 'bucketname1';
        const bucketName2 = 'bucketname2';
        const bucketName3 = 'bucketname3';
        const testbucketPutRequest1 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName1}.s3.amazonaws.com`}
        };
        const testbucketPutRequest2 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName2}.s3.amazonaws.com`}
        };
        const testbucketPutRequest3 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName3}.s3.amazonaws.com`}
        };
        const serviceGetRequest = {
            lowerCaseHeaders: {host: 's3.amazonaws.com'},
            url: '/',
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testbucketPutRequest1, next);
            },
            function waterfall2(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest2, next);
            },
            function waterfall3(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest3, next);
            },
            function waterfall4(result, next) {
                serviceGet(accessKey, metastore, serviceGetRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket)
                .to.have.length.of(3);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[0].Name[0])
                .to.equal(bucketName1);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[1].Name[0])
                .to.equal(bucketName2);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[2].Name[0])
                .to.equal(bucketName3);
            done();
        });
    });
});
