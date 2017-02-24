import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutWebsite from '../../../lib/api/bucketPutWebsite';
import bucketDeleteWebsite from '../../../lib/api/bucketDeleteWebsite';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    WebsiteConfig }
from '../helpers';
import metadata from '../../../lib/metadata/wrapper';
import configOfficial from '../../../lib/Config';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const locationConstraint = configOfficial.locationConstraints ?
  'aws-us-east-1' : 'us-east-1';
const config = new WebsiteConfig('index.html', 'error.html');
config.addRoutingRule({ ReplaceKeyPrefixWith: 'documents/' },
{ KeyPrefixEquals: 'docs/' });
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
const testBucketDeleteWebsiteRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/?website',
    query: { website: '' },
};
const testBucketPutWebsiteRequest = Object.assign({ post: config.getXml() },
    testBucketDeleteWebsiteRequest);

describe('deleteBucketWebsite API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest,
        locationConstraint, log, () => {
            bucketPutWebsite(authInfo, testBucketPutWebsiteRequest, log, done);
        });
    });
    afterEach(() => cleanup());

    it('should delete a bucket\'s website configuration in metadata', done => {
        bucketDeleteWebsite(authInfo, testBucketDeleteWebsiteRequest, log,
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
                assert.strictEqual(bucket.getWebsiteConfiguration(),
                    null);
                return done();
            });
        });
    });
});
