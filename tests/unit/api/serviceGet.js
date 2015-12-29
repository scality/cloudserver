import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import Config from '../../../lib/Config';
import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import serviceGet from '../../../lib/api/serviceGet';
import DummyRequestLogger from '../helpers';

const usersBucket = new Config().usersBucket;

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName1 = 'bucketname1';
const bucketName2 = 'bucketname2';
const bucketName3 = 'bucketname3';
// TODO: Remove references to metastore.  This is GH Issue #172
const metastore = undefined;

describe('serviceGet API', () => {
    beforeEach((done) => {
        metadata.deleteBucket('bucketname', () => {
            metadata.deleteBucket(usersBucket, () => {
                done();
            });
        });
    });

    afterEach((done) => {
        metadata.deleteBucket(bucketName1, () => {
            metadata.deleteBucket(bucketName2, () => {
                metadata.deleteBucket(bucketName3, () => {
                    metadata.deleteBucket(usersBucket, () => {
                        done();
                    });
                });
            });
        });
    });

    const serviceGetRequest = {
        lowerCaseHeaders: { host: 's3.amazonaws.com' },
        url: '/',
        headers: { host: 's3.amazonaws.com' },
    };

    it('should return the list of buckets owned by the user', (done) => {
        const bucketName1 = 'bucketname1';
        const bucketName2 = 'bucketname2';
        const bucketName3 = 'bucketname3';
        const testbucketPutRequest1 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            headers: {host: `${bucketName1}.s3.amazonaws.com`}
        };
        const testbucketPutRequest2 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
            headers: {host: `${bucketName2}.s3.amazonaws.com`}
        };
        const testbucketPutRequest3 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace,
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
                bucketPut(accessKey, metastore, testbucketPutRequest1, log,
                    next);
            },
            function waterfall2(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest2, log,
                    next);
            },
            function waterfall3(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest3, log,
                    next);
            },
            function waterfall4(result, next) {
                serviceGet(accessKey, metastore, serviceGetRequest, log,
                    next);
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
            metastore, serviceGetRequest, log, (err) => {
                assert.strictEqual(err, 'AccessDenied');
                done();
            });
    });
});
