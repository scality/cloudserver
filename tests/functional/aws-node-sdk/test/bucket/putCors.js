import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucketName = 'testcorsbucket';

const sampleCors = { CORSRules: [
  { AllowedMethods: ['PUT', 'POST', 'DELETE'],
    AllowedOrigins: ['http://www.example.com'],
    AllowedHeaders: ['*'],
    MaxAgeSeconds: 3000,
    ExposeHeaders: ['x-amz-server-side-encryption'] },
  { AllowedMethods: ['GET'],
    AllowedOrigins: ['*'],
    AllowedHeaders: ['*'],
    MaxAgeSeconds: 3000 },
] };

function _corsTemplate(params) {
    const sampleRule = {
        AllowedMethods: ['PUT', 'POST', 'DELETE'],
        AllowedOrigins: ['http://www.example.com'],
        AllowedHeaders: ['*'],
        MaxAgeSeconds: 3000,
        ExposeHeaders: ['x-amz-server-side-encryption'],
    };
    ['AllowedMethods', 'AllowedOrigins', 'AllowedHeaders', 'MaxAgeSeconds',
    'ExposeHeaders'].forEach(prop => {
        if (params[prop]) {
            sampleRule[prop] = params[prop];
        }
    });
    return { CORSRules: [sampleRule] };
}

describe('PUT bucket cors', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        function _testPutBucketCors(rules, statusCode, errMsg, cb) {
            s3.putBucketCors({ Bucket: bucketName,
                CORSConfiguration: rules }, err => {
                assert(err, 'Expected err but found none');
                assert.strictEqual(err.code, errMsg);
                assert.strictEqual(err.statusCode, statusCode);
                cb();
            });
        }

        beforeEach(done => s3.createBucket({ Bucket: bucketName }, done));

        afterEach(() => bucketUtil.deleteOne(bucketName));

        it('should put a bucket cors successfully', done => {
            s3.putBucketCors({ Bucket: bucketName,
                CORSConfiguration: sampleCors }, err => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                done();
            });
        });

        it('should return InvalidRequest if more than 100 rules', done => {
            const sampleRule = {
                AllowedMethods: ['PUT', 'POST', 'DELETE'],
                AllowedOrigins: ['http://www.example.com'],
                AllowedHeaders: ['*'],
                MaxAgeSeconds: 3000,
                ExposeHeaders: ['x-amz-server-side-encryption'],
            };
            const testCors = { CORSRules: [] };
            for (let i = 0; i < 101; i++) {
                testCors.CORSRules.push(sampleRule);
            }
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return MalformedXML if missing AllowedOrigin', done => {
            const testCors = _corsTemplate({ AllowedOrigins: [] });
            _testPutBucketCors(testCors, 400, 'MalformedXML', done);
        });

        it('should return InvalidRequest if more than one asterisk in ' +
        'AllowedOrigin', done => {
            const testCors =
                _corsTemplate({ AllowedOrigins: ['http://*.*.com'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return MalformedXML if missing AllowedMethod', done => {
            const testCors = _corsTemplate({ AllowedMethods: [] });
            _testPutBucketCors(testCors, 400, 'MalformedXML', done);
        });

        it('should return InvalidRequest if AllowedMethod is not a valid ' +
        'method', done => {
            const testCors = _corsTemplate({ AllowedMethods: ['test'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest for lowercase value for ' +
        'AllowedMethod', done => {
            const testCors = _corsTemplate({ AllowedMethods: ['put', 'get'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if more than one asterisk in ' +
        'AllowedHeader', done => {
            const testCors = _corsTemplate({ AllowedHeaders: ['*-amz-*'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if ExposeHeader has character ' +
        'that is not dash or alphanumeric',
        done => {
            const testCors = _corsTemplate({ ExposeHeaders: ['test header'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });

        it('should return InvalidRequest if ExposeHeader has wildcard',
        done => {
            const testCors = _corsTemplate({ ExposeHeaders: ['x-amz-*'] });
            _testPutBucketCors(testCors, 400, 'InvalidRequest', done);
        });
    });
});
