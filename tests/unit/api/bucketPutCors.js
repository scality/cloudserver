import assert from 'assert';
import { errors } from 'arsenal';

import bucketPut from '../../../lib/api/bucketPut';
import bucketPutCors from '../../../lib/api/bucketPutCors';
import { _validator,
    parseCorsXml } from '../../../lib/api/apiUtils/bucket/bucketCors';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    CorsConfigTester } from '../helpers';
import metadata from '../../../lib/metadata/wrapper';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _testPutBucketCors(authInfo, request, log, errCode, cb) {
    bucketPutCors(authInfo, request, log, err => {
        assert(err, 'Expected err but found none');
        assert.deepStrictEqual(err, errors[errCode]);
        cb();
    });
}

function _generateSampleXml(value) {
    const xml = '<CORSConfiguration>' +
    '<CORSRule>' +
    '<AllowedMethod>PUT</AllowedMethod>' +
    '<AllowedOrigin>www.example.com</AllowedOrigin>' +
    `${value}` +
    '</CORSRule>' +
    '</CORSConfiguration>';

    return xml;
}

describe('putBucketCORS API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with cors resource', done => {
        const corsUtil = new CorsConfigTester();
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName);
        bucketPutCors(authInfo, testBucketPutCorsRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting website config ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const uploadedCors = bucket.getCors();
                assert.deepStrictEqual(uploadedCors, corsUtil.getCors());
                return done();
            });
        });
    });

    it('should return BadDigest if md5 is omitted', done => {
        const corsUtil = new CorsConfigTester();
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName);
        testBucketPutCorsRequest.headers['content-md5'] = undefined;
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'BadDigest', done);
    });

    it('should return MalformedXML if body greater than 64KB', done => {
        const corsUtil = new CorsConfigTester();
        const body = Buffer.alloc(65537); // 64 * 1024 = 65536 bytes
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName, body);
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'MalformedXML', done);
    });

    it('should return InvalidRequest if more than one MaxAgeSeconds', done => {
        const corsUtil = new CorsConfigTester({ maxAgeSeconds: [60, 6000] });
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName);
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'MalformedXML', done);
    });
});

describe('PUT bucket cors :: helper validation functions ', () => {
    describe('validateNumberWildcards ', () => {
        it('should return expected values for test strings', done => {
            const testStrings = ['test', 'tes*t', 'tes**t'];
            const expectedResults = [true, true, false];

            for (let i = 0; i < testStrings.length; i++) {
                const result = _validator
                    .validateNumberWildcards(testStrings[i]);
                assert.strictEqual(result, expectedResults[i]);
            }
            done();
        });
    });

    describe('validateID ', () => {
        it('should validate successfully for valid ID', done => {
            const testValue = 'testid';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(typeof result[0].id, 'string');
                assert.strictEqual(result[0].id, testValue);
                return done();
            });
        });

        it('should return MalformedXML if more than one ID per rule', done => {
            const testValue = 'testid';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>` +
            `<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, err => {
                assert(err, 'Expected error but found none');
                assert.deepStrictEqual(err, errors.MalformedXML);
                return done();
            });
        });

        it('should validate & return undefined if empty value for ID', done => {
            const testValue = '';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(result[0].id, undefined);
                return done();
            });
        });

        it('should validate & return undefined if no ID element', done => {
            const xml = _generateSampleXml('');
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(result[0].id, undefined);
                return done();
            });
        });
    });

    describe('validateMaxAgeSeconds ', () => {
        it('should validate successfully for valid value', done => {
            const testValue = 60;
            const xml = _generateSampleXml(`<MaxAgeSeconds>${testValue}` +
                '</MaxAgeSeconds>');
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(typeof result[0].maxAgeSeconds, 'number');
                assert.strictEqual(result[0].maxAgeSeconds, testValue);
                return done();
            });
        });

        it('should return MalformedXML if more than one MaxAgeSeconds ' +
        'per rule', done => {
            const testValue = '60';
            const xml = _generateSampleXml(
                `<MaxAgeSeconds>${testValue}</MaxAgeSeconds>` +
                `<MaxAgeSeconds>${testValue}</MaxAgeSeconds>`);
            parseCorsXml(xml, log, err => {
                assert(err, 'Expected error but found none');
                assert.deepStrictEqual(err, errors.MalformedXML);
                return done();
            });
        });

        it('should validate & return undefined if empty value', done => {
            const testValue = '';
            const xml = _generateSampleXml(`<MaxAgeSeconds>${testValue}` +
                '</MaxAgeSeconds>');
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(result[0].MaxAgeSeconds, undefined);
                return done();
            });
        });

        it('should validate & return undefined if no MaxAgeSeconds', done => {
            const xml = _generateSampleXml('');
            parseCorsXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(result[0].id, undefined);
                return done();
            });
        });
    });
});
