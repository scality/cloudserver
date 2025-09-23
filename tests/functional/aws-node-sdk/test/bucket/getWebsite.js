const assert = require('assert');
const { S3Client,
    CreateBucketCommand,
    GetBucketWebsiteCommand,
    PutBucketWebsiteCommand,
    DeleteBucketCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const getConfig = require('../support/config');
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

// Helper function to delete bucket (replacing bucketUtil.deleteOne)
async function deleteBucket(s3, bucket) {
    try {
        await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    } catch (err) {
        // eslint-disable-next-line no-console
        console.log(err);
    }
}

describe('GET bucket website', () => {
    withV4(sigCfg => {
        const s3Config = getConfig('default', sigCfg);
        const s3 = new S3Client(s3Config);

        afterEach(() => deleteBucket(s3, bucketName));

        describe('with existing bucket configuration', () => {
            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutBucketWebsiteCommand({
                    Bucket: bucketName,
                    WebsiteConfiguration: config,
                }));
            });

            it('should return bucket website xml successfully', async () => {
                try {
                    const { $metadata, ...data } = await s3.send(new GetBucketWebsiteCommand({ Bucket: bucketName }));
                    const configObject = Object.assign({}, config);
                    assert.deepStrictEqual(data, configObject);
                    assert.strictEqual($metadata.httpStatusCode, 200);
                } catch (err) {
                    assert.fail(`Found unexpected err ${err}`);
                }
            });
        });

        describe('on bucket without website configuration', () => {
            before(async () => {
                process.stdout.write('about to create bucket\n');
                try {
                    await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                } catch (err) {
                    process.stdout.write('error creating bucket', err);
                    throw err;
                }
            });

            it('should return NoSuchWebsiteConfiguration', async () => {
                try {
                    await s3.send(new GetBucketWebsiteCommand({ Bucket: bucketName }));
                    assert.fail('Expected NoSuchWebsiteConfiguration error');
                } catch (err) {
                    assert(err);
                    assert.strictEqual(err.Code, 'NoSuchWebsiteConfiguration');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });
    });
});
