const assert = require('assert');
const crypto = require('crypto');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const bucketGetCors = require('../../../lib/api/bucketGetCors');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
= require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetCorsTestBucket';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _makeCorsRequest(xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?cors',
        query: { cors: '' },
    };

    if (xml) {
        request.post = xml;
        request.headers['content-md5'] = crypto.createHash('md5')
            .update(request.post, 'utf8').digest('base64');
    }
    return request;
}
const testGetCorsRequest = _makeCorsRequest();

function _comparePutGetXml(sampleXml, done) {
    const fullXml = '<?xml version="1.0" encoding="UTF-8" ' +
    'standalone="yes"?><CORSConfiguration>' +
    `${sampleXml}</CORSConfiguration>`;
    const testPutCorsRequest = _makeCorsRequest(fullXml);
    bucketPutCors(authInfo, testPutCorsRequest, log, err => {
        if (err) {
            process.stdout.write(`Err putting cors config ${err}`);
            return done(err);
        }
        return bucketGetCors(authInfo, testGetCorsRequest, log,
        (err, res) => {
            assert.strictEqual(err, null, `Unexpected err ${err}`);
            assert.strictEqual(res, fullXml);
            done();
        });
    });
}

describe('getBucketCors API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should return same XML as uploaded for AllowedMethod and ' +
    'AllowedOrigin', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedMethod>POST</AllowedMethod>' +
            '<AllowedMethod>DELETE</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '<AllowedOrigin>http://www.pusheen.com</AllowedOrigin>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });

    it('should return same XML as uploaded for multiple rules', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '</CORSRule>' +
            '<CORSRule>' +
            '<AllowedMethod>POST</AllowedMethod>' +
            '<AllowedOrigin>http://www.pusheen.com</AllowedOrigin>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });

    it('should return same XML as uploaded for AllowedHeader\'s', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '<AllowedHeader>Content-Length</AllowedHeader>' +
            '<AllowedHeader>Expires</AllowedHeader>' +
            '<AllowedHeader>Content-Encoding</AllowedHeader>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });

    it('should return same XML as uploaded for ExposedHeader\'s', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '<ExposeHeader>Content-Length</ExposeHeader>' +
            '<ExposeHeader>Expires</ExposeHeader>' +
            '<ExposeHeader>Content-Encoding</ExposeHeader>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });

    it('should return same XML as uploaded for ID', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '<ID>testid</ID>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });

    it('should return same XML as uploaded for MaxAgeSeconds', done => {
        const sampleXml =
            '<CORSRule>' +
            '<AllowedMethod>PUT</AllowedMethod>' +
            '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
            '<MaxAgeSeconds>600</MaxAgeSeconds>' +
            '</CORSRule>';
        _comparePutGetXml(sampleXml, done);
    });
});
