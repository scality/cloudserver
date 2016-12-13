import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const bucketName = 'testbucketwebsitebucket';

describe('PUT bucket website', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _testPutBucketWebsite(config, statusCode, errMsg, cb) {
            s3.putBucketWebsite({ Bucket: bucketName,
                WebsiteConfiguration: config }, err => {
                assert(err, 'Expected err but found none');
                assert.strictEqual(err.code, errMsg);
                assert.strictEqual(err.statusCode, statusCode);
                cb();
            });
        }
        beforeEach(done => {
            process.stdout.write('about to create bucket\n');
            s3.createBucket({ Bucket: bucketName }, err => {
                if (err) {
                    process.stdout.write('error in beforeEach', err);
                    done(err);
                }
                done();
            });
        });

        afterEach(() => {
            process.stdout.write('about to empty bucket\n');
            return bucketUtil.empty(bucketName).then(() => {
                process.stdout.write('about to delete bucket\n');
                return bucketUtil.deleteOne(bucketName);
            }).catch(err => {
                if (err) {
                    process.stdout.write('error in afterEach', err);
                    throw err;
                }
            });
        });

        it('should put a bucket website successfully', done => {
            const config = new WebsiteConfigTester('index.html');
            s3.putBucketWebsite({ Bucket: bucketName,
                WebsiteConfiguration: config }, err => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                done();
            });
        });

        it('should return InvalidArgument if IndexDocument or ' +
        'RedirectAllRequestsTo is not provided', done => {
            const config = new WebsiteConfigTester();
            _testPutBucketWebsite(config, 400, 'InvalidArgument', done);
        });

        it('should return an InvalidRequest if both ' +
        'RedirectAllRequestsTo and IndexDocument are provided', done => {
            const redirectAllTo = {
                HostName: 'test',
                Protocol: 'http',
            };
            const config = new WebsiteConfigTester(null, null,
            redirectAllTo);
            config.addRoutingRule({ Protocol: 'http' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidArgument if index has slash', done => {
            const config = new WebsiteConfigTester('in/dex.html');
            _testPutBucketWebsite(config, 400, 'InvalidArgument', done);
        });

        it('should return InvalidRequest if both ReplaceKeyWith and ' +
        'ReplaceKeyPrefixWith are present in same rule', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ ReplaceKeyPrefixWith: 'test',
            ReplaceKeyWith: 'test' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if both ReplaceKeyWith and ' +
        'ReplaceKeyPrefixWith are present in same rule', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ ReplaceKeyPrefixWith: 'test',
            ReplaceKeyWith: 'test' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if Redirect Protocol is ' +
        'not http or https', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ Protocol: 'notvalidprotocol' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if RedirectAllRequestsTo Protocol ' +
        'is not http or https', done => {
            const redirectAllTo = {
                HostName: 'test',
                Protocol: 'notvalidprotocol',
            };
            const config = new WebsiteConfigTester(null, null, redirectAllTo);
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return MalformedXML if Redirect HttpRedirectCode ' +
        'is a string that does not contains a number', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ HttpRedirectCode: 'notvalidhttpcode' });
            _testPutBucketWebsite(config, 400, 'MalformedXML', done);
        });

        it('should return InvalidRequest if Redirect HttpRedirectCode ' +
        'is not a valid http redirect code (3XX excepting 300)', done => {
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ HttpRedirectCode: '400' });
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if Condition ' +
        'HttpErrorCodeReturnedEquals is a string that does ' +
        ' not contain a number', done => {
            const condition = { HttpErrorCodeReturnedEquals: 'notvalidcode' };
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ HostName: 'test' }, condition);
            _testPutBucketWebsite(config, 400, 'MalformedXML', done);
        });

        it('should return InvalidRequest if Condition ' +
        'HttpErrorCodeReturnedEquals is not a valid http' +
        'error code (4XX or 5XX)', done => {
            const condition = { HttpErrorCodeReturnedEquals: '300' };
            const config = new WebsiteConfigTester('index.html');
            config.addRoutingRule({ HostName: 'test' }, condition);
            _testPutBucketWebsite(config, 400, 'InvalidRequest', done);
        });
    });
});
