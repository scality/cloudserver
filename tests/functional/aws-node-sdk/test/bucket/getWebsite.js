const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const bucketName = 'testgetwebsitetestbucket';
const ruleRedirect1 = {
    HostName: 'test',
    ReplaceKeyPrefixWith: 'documents/',
};
const ruleCondition1 = {
    KeyPrefixEquals: 'docs/',
};
const ruleRedirect2 = {
    HttpRedirectCode: '302',
};
const ruleCondition2 = {
    HttpErrorCodeReturnedEquals: '404',
};
const config = new WebsiteConfigTester('index.html', 'error.html');
config.addRoutingRule(ruleRedirect1, ruleCondition1);
config.addRoutingRule(ruleRedirect2, ruleCondition2);

describe('GET bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        afterEach(() => bucketUtil.deleteOne(bucketName));

        describe('with existing bucket configuration', () => {
            before(() =>
                s3.createBucketAsync({ Bucket: bucketName })
                .then(() => s3.putBucketWebsiteAsync({
                    Bucket: bucketName,
                    WebsiteConfiguration: config,
                })));

            it('should return bucket website xml successfully', done => {
                s3.getBucketWebsite({ Bucket: bucketName }, (err, data) => {
                    assert.strictEqual(err, null,
                        `Found unexpected err ${err}`);
                    const configObject = Object.assign({}, config);
                    assert.deepStrictEqual(data, configObject);
                    return done();
                });
            });
        });

        describe('on bucket without website configuration', () => {
            before(done => {
                process.stdout.write('about to create bucket\n');
                s3.createBucket({ Bucket: bucketName }, err => {
                    if (err) {
                        process.stdout.write('error creating bucket', err);
                        return done(err);
                    }
                    return done();
                });
            });

            it('should return NoSuchWebsiteConfiguration', done => {
                s3.getBucketWebsite({ Bucket: bucketName }, err => {
                    assert(err);
                    assert.strictEqual(err.code, 'NoSuchWebsiteConfiguration');
                    assert.strictEqual(err.statusCode, 404);
                    return done();
                });
            });
        });
    });
});
