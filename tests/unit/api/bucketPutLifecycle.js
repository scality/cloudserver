const assert = require('assert');
const { errors } = require('arsenal');

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
    actionImplicitDenies: false,
};

const expectedLifecycleConfig = {
    rules: [
        {
            ruleID: 'test-id1',
            ruleStatus: 'Enabled',
            prefix: 'test-prefix',
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

    describe('checksum validation', () => {
        const lifecycleXml = '<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            '<Rule>' +
            '<ID>test-rule</ID>' +
            '<Status>Enabled</Status>' +
            '<Prefix>test/</Prefix>' +
            '<Expiration><Days>30</Days></Expiration>' +
            '</Rule>' +
            '</LifecycleConfiguration>';

        it('should not return an error when Content-MD5 header is missing', done => {
            const testLifecycleRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: lifecycleXml,
                url: '/?lifecycle',
                query: { lifecycle: '' },
                actionImplicitDenies: false,
            };

            bucketPutLifecycle(authInfo, testLifecycleRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should return BadDigest error when Content-MD5 header mismatches', done => {
            const testLifecycleRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': '+5yj3kZsXledyKr18eaUDg==', // incorrect MD5
                },
                post: lifecycleXml,
                url: '/?lifecycle',
                query: { lifecycle: '' },
                actionImplicitDenies: false,
            };

            bucketPutLifecycle(authInfo, testLifecycleRequest, log, err => {
                assert.deepStrictEqual(err, errors.BadDigest);
                done();
            });
        });

        it('should not return an error when Content-MD5 header matches', done => {
            const testLifecycleRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': 'atetz1xBS6pZndwhthYINg==', // correct MD5
                },
                post: lifecycleXml,
                url: '/?lifecycle',
                query: { lifecycle: '' },
                actionImplicitDenies: false,
            };

            bucketPutLifecycle(authInfo, testLifecycleRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
