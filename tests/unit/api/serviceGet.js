import assert from 'assert';
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

    const serviceGetRequest = {
        lowerCaseHeaders: {host: 's3.amazonaws.com'},
        url: '/',
        headers: {host: 's3.amazonaws.com'},
    };

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
        const date = new Date();
        let month = (date.getMonth() + 1).toString();
        if (month.length === 1) {
            month = `0${month}`;
        }
        const dateString = `${date.getFullYear()}-${month}-${date.getDate()}`;

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
            assert.strictEqual(result.ListAllMyBucketsResult
                .Buckets[0].Bucket.length, 3);
            assert.strictEqual(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[0].Name[0], bucketName1);
            assert.strictEqual(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[1].Name[0], bucketName2);
            assert.strictEqual(result.ListAllMyBucketsResult
                .Buckets[0].Bucket[2].Name[0], bucketName3);
            assert.strictEqual(result.ListAllMyBucketsResult.$.xmlns,
                `http://s3.amazonaws.com/doc/${dateString}`);
            done();
        });
    });

    it('should prevent anonymous user from accessing getService API', done => {
        serviceGet('http://acs.amazonaws.com/groups/global/AllUsers',
            metastore, serviceGetRequest, (err) => {
                assert.strictEqual(err, 'AccessDenied');
                done();
            });
    });
});
