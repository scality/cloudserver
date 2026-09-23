const assert = require('assert');
const http = require('http');

const bucket = 'test-bucket';
const objectKey = 'test-file.txt';

describe('malformed Date header:', () => {
    it('should return AccessDenied for bad date with x-amz-content-sha256 header', done => {
        const options = {
            hostname: 'localhost',
            port: 8000,
            path: `/${bucket}/${objectKey}`,
            method: 'GET',
            headers: {
                Date: 'BAD_DATE',
                Authorization:
                    'AWS4-HMAC-SHA256 Credential=accessKey1/20260211/us-east-1/s3/aws4_request, ' +
                    'SignedHeaders=host, Signature=d459d5b2a2395b4c65d8f8aa2729b22c5abb04614fafbd93ab4fe203e76d21a3',
                'X-Amz-Content-Sha256': 'fa8d015f89da2a769d1cea7e3bd77a5670d098d7844cda148a40c1304e5b778b',
                Host: 'localhost:8000',
            },
        };

        const req = http.request(options, res => {
            let body = '';
            res.on('data', chunk => {
                body += chunk;
            });
            res.on('end', () => {
                assert.strictEqual(res.statusCode, 403, 'Server should return 403 AccessDenied for malformed Date');
                assert(body.includes('AccessDenied'), 'Response should contain AccessDenied');
                assert(body.includes('Authentication requires a valid Date or x-amz-date header'));
                done();
            });
        });

        req.on('error', err => {
            // If we get ECONNRESET or similar, it means the server crashed
            assert.fail(`Server crashed or connection error: ${err.message}`);
        });

        req.end();
    });

    it('should return AccessDenied for bad x-amz-date with x-amz-content-sha256 header', done => {
        const options = {
            hostname: 'localhost',
            port: 8000,
            path: `/${bucket}/${objectKey}`,
            method: 'GET',
            headers: {
                'X-Amz-Date': 'BAD_DATE',
                Authorization:
                    'AWS4-HMAC-SHA256 Credential=accessKey1/20260211/us-east-1/s3/aws4_request, ' +
                    'SignedHeaders=host;x-amz-date, ' +
                    'Signature=d459d5b2a2395b4c65d8f8aa2729b22c5abb04614fafbd93ab4fe203e76d21a3',
                'X-Amz-Content-Sha256': 'fa8d015f89da2a769d1cea7e3bd77a5670d098d7844cda148a40c1304e5b778b',
                Host: 'localhost:8000',
            },
        };

        const req = http.request(options, res => {
            let body = '';
            res.on('data', chunk => {
                body += chunk;
            });
            res.on('end', () => {
                assert.strictEqual(res.statusCode, 403, 'Server should return 403 AccessDenied for malformed Date');
                assert(body.includes('AccessDenied'), 'Response should contain AccessDenied');
                assert(body.includes('Authentication requires a valid Date or x-amz-date header'));
                done();
            });
        });

        req.on('error', err => {
            // If we get ECONNRESET or similar, it means the server crashed
            assert.fail(`Server crashed or connection error: ${err.message}`);
        });

        req.end();
    });
});
