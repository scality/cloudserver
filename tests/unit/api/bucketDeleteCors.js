const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const bucketDeleteCors = require('../../../lib/api/bucketDeleteCors');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    CorsConfigTester } = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

const corsUtil = new CorsConfigTester();

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
const testBucketPutCorsRequest =
    corsUtil.createBucketCorsRequest('PUT', bucketName);
const testBucketDeleteCorsRequest =
    corsUtil.createBucketCorsRequest('DELETE', bucketName);

describe('deleteBucketCors API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, () => {
            bucketPutCors(authInfo, testBucketPutCorsRequest, log, done);
        });
    });
    afterEach(() => cleanup());

    it('should delete a bucket\'s cors configuration in metadata', done => {
        bucketDeleteCors(authInfo, testBucketDeleteCorsRequest, log,
        err => {
            if (err) {
                process.stdout.write(`Unexpected err ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                assert.strictEqual(bucket.getCors(), null);
                return done();
            });
        });
    });
});
