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

describe('GET bucket website', () => {
    withV4(sigCfg => {
        const s3Config = getConfig('default', sigCfg);
        const s3 = new S3Client(s3Config);

        afterEach(() =>  s3.send(new DeleteBucketCommand({ Bucket: bucketName })));

        describe('with existing bucket configuration', () => {
            before(async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucketName }));
                await s3.send(new PutBucketWebsiteCommand({
                    Bucket: bucketName,
                    WebsiteConfiguration: config,
                }));
            });

            it('should return bucket website xml successfully', async () => {
                const { $metadata, ...data } = await s3.send(new GetBucketWebsiteCommand({ Bucket: bucketName }));
                const configObject = Object.assign({}, config);
                assert.deepStrictEqual(data, configObject);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        describe('on bucket without website configuration', () => {
            before(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));

            it('should return NoSuchWebsiteConfiguration', async () => {
                try {
                    await s3.send(new GetBucketWebsiteCommand({ Bucket: bucketName }));
                    assert.fail('Expected NoSuchWebsiteConfiguration error');
                } catch (err) {
                    assert.strictEqual(err.name, 'NoSuchWebsiteConfiguration');
                    assert.strictEqual(err.$metadata.httpStatusCode, 404);
                }
            });
        });
    });
});
