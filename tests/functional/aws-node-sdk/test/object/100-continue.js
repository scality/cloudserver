const assert = require('assert');
const http = require('http');
const https = require('https');
const url = require('url');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const conf = require('../../../../../lib/Config').config;

const transport = conf.https ? https : http;
const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const hostname = process.env.AWS_ON_AIR ? 's3.amazonaws.com' : ipAddress;
const port = process.env.AWS_ON_AIR ? 80 : 8000;
const bucket = 'foo-bucket';
const key = 'foo-key';
const body = Buffer.alloc(1024 * 1024);

class ContinueRequestHandler {
    constructor(path) {
        this.path = path;
        return this;
    }

    setRequestPath(path) {
        this.path = path;
        return this;
    }

    setExpectHeader(header) {
        this.expectHeader = header;
        return this;
    }

    getRequestOptions() {
        return {
            path: this.path,
            hostname,
            port,
            method: 'PUT',
            headers: {
                'content-length': body.length,
                'Expect': this.expectHeader || '100-continue',
            },
        };
    }

    hasStatusCode(statusCode, cb) {
        const options = this.getRequestOptions();
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
        const req = transport.request(options, res => {
            assert.strictEqual(res.statusCode, statusCode);
            return cb();
        });
        // Send the body either on the continue event, or immediately.
        if (this.expectHeader === '100-continue') {
            req.on('continue', () => req.end(body));
        } else {
            req.end(body);
        }
    }

    sendsBodyOnContinue(cb) {
        const options = this.getRequestOptions();
        process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';
        const req = transport.request(options);
        // At this point we have only sent the header.
        assert(req.output.length === 1);
        const headerLen = req.output[0].length;
        req.on('continue', () => {
            // Has only the header been sent?
            assert.strictEqual(req.socket.bytesWritten, headerLen);
            // Send the body since the continue event has been emitted.
            return req.end(body);
        });
        req.on('close', () => {
            const expected = body.length + headerLen;
            // Has the entire body been sent?
            assert.strictEqual(req.socket.bytesWritten, expected);
            return cb();
        });
        req.on('error', err => cb(err));
    }
}

describe('PUT public object with 100-continue header', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let continueRequest;
        const invalidSignedURL = `/${bucket}/${key}`;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const params = {
                Bucket: bucket,
                Key: key,
            };
            const signedUrl = s3.getSignedUrl('putObject', params);
            const { path } = url.parse(signedUrl);
            continueRequest = new ContinueRequestHandler(path);
            return s3.createBucketAsync({ Bucket: bucket });
        });

        afterEach(() =>
            bucketUtil.empty(bucket)
            .then(() => bucketUtil.deleteOne(bucket)));

        it('should return 200 status code', done =>
            continueRequest.hasStatusCode(200, done));

        it('should return 200 status code with upper case value', done =>
            continueRequest.setExpectHeader('100-CONTINUE')
                .hasStatusCode(200, done));

        it('should return 200 status code if incorrect value', done =>
            continueRequest.setExpectHeader('101-continue')
                .hasStatusCode(200, done));

        it('should return 403 status code if cannot authenticate', done =>
            continueRequest.setRequestPath(invalidSignedURL)
                .hasStatusCode(403, done));

        it('should wait for continue event before sending body', done =>
            continueRequest.sendsBodyOnContinue(done));

        it('should continue if a public user', done =>
            continueRequest.setRequestPath(invalidSignedURL)
                .sendsBodyOnContinue(done));
    });
});
