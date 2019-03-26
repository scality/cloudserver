const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const { _validator, parseCorsXml }
    = require('../../../lib/api/apiUtils/bucket/bucketCors');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    CorsConfigTester }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

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
        expect(err).toBeTruthy();
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

    test('should update a bucket\'s metadata with cors resource', done => {
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

    test('should return BadDigest if md5 is omitted', done => {
        const corsUtil = new CorsConfigTester();
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName);
        testBucketPutCorsRequest.headers['content-md5'] = undefined;
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'BadDigest', done);
    });

    test('should return MalformedXML if body greater than 64KB', done => {
        const corsUtil = new CorsConfigTester();
        const body = Buffer.alloc(65537); // 64 * 1024 = 65536 bytes
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName, body);
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'MalformedXML', done);
    });

    test('should return InvalidRequest if more than one MaxAgeSeconds', done => {
        const corsUtil = new CorsConfigTester({ maxAgeSeconds: [60, 6000] });
        const testBucketPutCorsRequest = corsUtil
            .createBucketCorsRequest('PUT', bucketName);
        _testPutBucketCors(authInfo, testBucketPutCorsRequest,
            log, 'MalformedXML', done);
    });
});

describe('PUT bucket cors :: helper validation functions ', () => {
    describe('validateNumberWildcards ', () => {
        test('should return expected values for test strings', done => {
            const testStrings = ['test', 'tes*t', 'tes**t'];
            const expectedResults = [true, true, false];

            for (let i = 0; i < testStrings.length; i++) {
                const result = _validator
                    .validateNumberWildcards(testStrings[i]);
                expect(result).toBe(expectedResults[i]);
            }
            done();
        });
    });

    describe('validateID ', () => {
        test('should validate successfully for valid ID', done => {
            const testValue = 'testid';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(typeof result[0].id).toBe('string');
                expect(result[0].id).toBe(testValue);
                return done();
            });
        });

        test('should return MalformedXML if more than one ID per rule', done => {
            const testValue = 'testid';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>` +
            `<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, err => {
                expect(err).toBeTruthy();
                assert.deepStrictEqual(err, errors.MalformedXML);
                return done();
            });
        });

        test('should validate & return undefined if empty value for ID', done => {
            const testValue = '';
            const xml = _generateSampleXml(`<ID>${testValue}</ID>`);
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(result[0].id).toBe(undefined);
                return done();
            });
        });

        test('should validate & return undefined if no ID element', done => {
            const xml = _generateSampleXml('');
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(result[0].id).toBe(undefined);
                return done();
            });
        });
    });

    describe('validateMaxAgeSeconds ', () => {
        test('should validate successfully for valid value', done => {
            const testValue = 60;
            const xml = _generateSampleXml(`<MaxAgeSeconds>${testValue}` +
                '</MaxAgeSeconds>');
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(typeof result[0].maxAgeSeconds).toBe('number');
                expect(result[0].maxAgeSeconds).toBe(testValue);
                return done();
            });
        });

        test('should return MalformedXML if more than one MaxAgeSeconds ' +
        'per rule', done => {
            const testValue = '60';
            const xml = _generateSampleXml(
                `<MaxAgeSeconds>${testValue}</MaxAgeSeconds>` +
                `<MaxAgeSeconds>${testValue}</MaxAgeSeconds>`);
            parseCorsXml(xml, log, err => {
                expect(err).toBeTruthy();
                assert.deepStrictEqual(err, errors.MalformedXML);
                return done();
            });
        });

        test('should validate & return undefined if empty value', done => {
            const testValue = '';
            const xml = _generateSampleXml(`<MaxAgeSeconds>${testValue}` +
                '</MaxAgeSeconds>');
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(result[0].MaxAgeSeconds).toBe(undefined);
                return done();
            });
        });

        test('should validate & return undefined if no MaxAgeSeconds', done => {
            const xml = _generateSampleXml('');
            parseCorsXml(xml, log, (err, result) => {
                expect(err).toBe(null);
                expect(result[0].id).toBe(undefined);
                return done();
            });
        });
    });
});
