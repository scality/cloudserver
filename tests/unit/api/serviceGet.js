import assert from 'assert';
import async from 'async';
import { parseString } from 'xml2js';

import constants from '../../../constants';
import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import serviceGet from '../../../lib/api/serviceGet';
import DummyRequestLogger from '../helpers';

const usersBucket = constants.usersBucket;

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const namespace = 'default';
const bucketName1 = 'bucketname1';
const bucketName2 = 'bucketname2';
const bucketName3 = 'bucketname3';

describe('serviceGet API', () => {
    beforeEach((done) => {
        metadata.deleteBucket('bucketname', log, () => {
            metadata.deleteBucket(usersBucket, log, () => {
                done();
            });
        });
    });

    afterEach((done) => {
        metadata.deleteBucket(bucketName1, log, () => {
            metadata.deleteBucket(bucketName2, log, () => {
                metadata.deleteBucket(bucketName3, log, () => {
                    metadata.deleteBucket(usersBucket, log, () => {
                        done();
                    });
                });
            });
        });
    });

    const serviceGetRequest = {
        parsedHost: 's3.amazonaws.com',
        lowerCaseHeaders: { host: 's3.amazonaws.com' },
        url: '/',
        headers: { host: 's3.amazonaws.com' },
    };

    it('should return the list of buckets owned by the user', (done) => {
        const bucketName1 = 'bucketname1';
        const bucketName2 = 'bucketname2';
        const bucketName3 = 'bucketname3';
        const testbucketPutRequest1 = {
            namespace,
            bucketName: bucketName1,
            lowerCaseHeaders: {},
            url: '/',
            headers: {host: `${bucketName1}.s3.amazonaws.com`}
        };
        const testbucketPutRequest2 = {
            namespace,
            bucketName: bucketName2,
            lowerCaseHeaders: {},
            url: '/',
            headers: {host: `${bucketName2}.s3.amazonaws.com`}
        };
        const testbucketPutRequest3 = {
            namespace,
            bucketName: bucketName3,
            lowerCaseHeaders: {},
            url: '/',
            headers: {host: `${bucketName3}.s3.amazonaws.com`}
        };
        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey,  testbucketPutRequest1, log,
                    next);
            },
            function waterfall2(result, next) {
                bucketPut(accessKey,  testbucketPutRequest2, log,
                    next);
            },
            function waterfall3(result, next) {
                bucketPut(accessKey,  testbucketPutRequest3, log,
                    next);
            },
            function waterfall4(result, next) {
                serviceGet(accessKey,  serviceGetRequest, log,
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
                `http://s3.amazonaws.com/doc/2006-03-01/`);
            done();
        });
    });

    it('should prevent anonymous user from accessing getService API', done => {
        serviceGet('http://acs.amazonaws.com/groups/global/AllUsers',
             serviceGetRequest, log, (err) => {
                 assert.strictEqual(err, 'AccessDenied');
                 done();
             });
    });
});
