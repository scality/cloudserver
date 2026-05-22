const assert = require('assert');
const { CreateBucketCommand, PutBucketWebsiteCommand } = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const bucketName = 'testbucketwebsitebucket';

describe('PUT bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _testPutBucketWebsite(config, statusCode, errMsg, cb) {
            s3.send(new PutBucketWebsiteCommand({ Bucket: bucketName, WebsiteConfiguration: config }))
                .then(() => {
                    cb(new Error('Expected err but found none'));
                })
                .catch(err => {
                    assert.strictEqual(err.name, errMsg);
                    assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
                    cb();
                });
        }
        beforeEach(() => s3.send(new CreateBucketCommand({ Bucket: bucketName })));

        afterEach(async () => {
            await bucketUtil.empty(bucketName);
            await bucketUtil.deleteOne(bucketName);
        });

        it('should put a bucket website successfully', () => {
            const config = new WebsiteConfigTester('index.html');
            s3.send(new PutBucketWebsiteCommand({ Bucket: bucketName, WebsiteConfiguration: config }));
        });

        it('should return InvalidArgument if IndexDocument or ' + 'RedirectAllRequestsTo is not provided', done => {
            const config = new WebsiteConfigTester();
            _testPutBucketWebsite(config, 400, 'InvalidArgument', done);
        });

        it(
            'should return an InvalidRequest if both ' + 'RedirectAllRequestsTo and IndexDocument are provided',
            done => {
                const redirectAllTo = {
                    HostName: 'test',
                    Protocol: 'http',
                };
                const config = new WebsiteConfigTester(null, null, redirectAllTo);
                config.addRoutingRule({ Protocol: 'http' });
                _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
            },
        );

        it('should return InvalidArgument if index has slash', done => {
            const config = new WebsiteConfigTester('in/dex.html');
            _testPutBucketWebsite(config, 400, 'InvalidArgument', done);
        });

        it(
            'should return InvalidRequest if both ReplaceKeyWith and ' +
                'ReplaceKeyPrefixWith are present in same rule',
            done => {
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ ReplaceKeyPrefixWith: 'test', ReplaceKeyWith: 'test' });
                _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
            },
        );

        it(
            'should return InvalidRequest if both ReplaceKeyWith and ' +
                'ReplaceKeyPrefixWith are present in same rule',
            done => {
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ ReplaceKeyPrefixWith: 'test', ReplaceKeyWith: 'test' });
                _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
            },
        );

        it('should return InvalidRequest if Redirect Protocol is ' + 'not http or https', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ Protocol: 'notvalidprotocol' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if RedirectAllRequestsTo Protocol ' + 'is not http or https', done => {
            const redirectAllTo = {
                HostName: 'test',
                Protocol: 'notvalidprotocol',
            };
            const config = new WebsiteConfigTester(null, null, redirectAllTo);
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it(
            'should return MalformedXML if Redirect HttpRedirectCode ' + 'is a string that does not contains a number',
            done => {
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ HttpRedirectCode: 'notvalidhttpcode' });
                _testPutBucketWebsite(config, 400, 'MalformedXML', done);
            },
        );

        it(
            'should return InvalidRequest if Redirect HttpRedirectCode ' +
                'is not a valid http redirect code (3XX excepting 300)',
            done => {
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ HttpRedirectCode: '400' });
                _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
            },
        );

        it(
            'should return InvalidRequest if Condition ' +
                'HttpErrorCodeReturnedEquals is a string that does ' +
                ' not contain a number',
            done => {
                const condition = { HttpErrorCodeReturnedEquals: 'notvalidcode' };
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ HostName: 'test' }, condition);
                _testPutBucketWebsite(config, 400, 'MalformedXML', done);
            },
        );

        it(
            'should return InvalidRequest if Condition ' +
                'HttpErrorCodeReturnedEquals is not a valid http' +
                'error code (4XX or 5XX)',
            done => {
                const condition = { HttpErrorCodeReturnedEquals: '300' };
                const config = new WebsiteConfigTester('index.html');
                config.addRoutingRule({ HostName: 'test' }, condition);
                _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
            },
        );
    });
});
