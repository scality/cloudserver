const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutLifecycle = require('../../../lib/api/bucketPutLifecycle');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const { getLifecycleRequest, getLifecycleXml } =
    require('../utils/lifecycleHelpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const expectedLifecycleConfig = {
    rules: [
        {
            ruleID: 'test-id1',
            ruleStatus: 'Enabled',
            filter: {
                rulePrefix: 'test-prefix',
            },
            actions: [
                {
                    actionName: 'AbortIncompleteMultipartUpload',
                    days: 30,
                },
            ],
        },
        {
            ruleID: 'test-id2',
            ruleStatus: 'Enabled',
            filter: {
                rulePrefix: 'test-prefix',
                tags: [
                    {
                        key: 'test-key1',
                        val: 'test-value1',
                    },
                    {
                        key: 'test-key2',
                        val: 'test-value2',
                    },
                ],
            },
            actions: [
                {
                    actionName: 'NoncurrentVersionExpiration',
                    days: 1,
                },
            ],
        },
        {
            ruleID: 'test-id3',
            ruleStatus: 'Disabled',
            filter: {
                rulePrefix: '',
                tags: [
                    {
                        key: 'test-key1',
                        val: 'test-value1',
                    },
                ],
            },
            actions: [
                {
                    actionName: 'Expiration',
                    days: 365,
                },
            ],
        },
    ],
};

describe('putBucketLifecycle API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with lifecycle config obj', done => {
        const testPutLifecycleRequest = getLifecycleRequest(bucketName,
            getLifecycleXml());
        bucketPutLifecycle(authInfo, testPutLifecycleRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting lifecycle config ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const bucketLifecycleConfig =
                    bucket.getLifecycleConfiguration();
                assert.deepStrictEqual(
                    bucketLifecycleConfig, expectedLifecycleConfig);
                return done();
            });
        });
    });
});
