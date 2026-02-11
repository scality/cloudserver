const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { makeS3Request } = require('../utils/makeRequest');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');
const url = require('url');

const bucket = 'testunsignedcontentshabucket';
const objectKey = 'key';
const objData = Buffer.alloc(1024, 'a');

const config = require('../../config.json');
const authCredentials = {
    accessKey: config.accessKey,
    secretKey: config.secretKey,
};

class HttpRequestAuthV4NoSHA256SignedHeader extends HttpRequestAuthV4 {
    constructor(url, params, callback) {
        super(url, params, callback);
    }

    _constructRequest() {
        const dateObj = new Date();
        const isoDate = dateObj.toISOString();
        this._timestamp = [
            isoDate.slice(0, 4),
            isoDate.slice(5, 7),
            isoDate.slice(8, 13),
            isoDate.slice(14, 16),
            isoDate.slice(17, 19),
            'Z',
        ].join('');

        const urlObj = new url.URL(this._url);
        const signedHeaders = {
            'host': urlObj.host,
            'x-amz-date': this._timestamp,
        };
        const httpHeaders = Object.assign({}, this._httpParams.headers);
        Object.keys(httpHeaders).forEach(header => {
            const lowerHeader = header.toLowerCase();
            if (!['connection', 'transfer-encoding', 'x-amz-content-sha256'].includes(lowerHeader)) {
                signedHeaders[lowerHeader] = httpHeaders[header];
            }
        });
        httpHeaders.Authorization = 
            this.getAuthorizationHeader(urlObj, signedHeaders, httpHeaders['x-amz-content-sha256']);
        return Object.assign(httpHeaders, signedHeaders);
    }
}

describe('unsigned x-amz-content-sha256 header in AuthV4 requests:', () => {
    before(done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        async.series([
            next => makeS3Request({
                method: 'DELETE',
                authCredentials,
                bucket,
                objectKey,
            }, next),
            next => makeS3Request({
                method: 'DELETE',
                authCredentials,
                bucket,
            }, next),
        ], err => {
            assert.ifError(err);
            done();
        });
    });

    it('should accept x-amz-content-sha256 header not in SignedHeaders list', done => {
        // Calculate the SHA256 hash of the data
        const contentSha256 = crypto.createHash('sha256')
            .update(objData)
            .digest('hex');

        const req = new HttpRequestAuthV4NoSHA256SignedHeader(
            `http://localhost:8000/${bucket}/${objectKey}`,
            Object.assign(
                {
                    method: 'PUT',
                    headers: {
                        'content-length': objData.length,
                        'x-amz-content-sha256': contentSha256,
                    },
                },
                authCredentials
            ),
            res => {
                let body = '';
                res.on('data', chunk => {
                    body += chunk;
                });
                res.on('end', () => {
                    assert.strictEqual(body, '', 'expected empty body');
                    assert.strictEqual(res.statusCode, 200,
                        'Request should succeed even when x-amz-content-sha256 is not signed');
                    done();
                });
            }
        );

        req.on('error', err => {
            assert.ifError(err);
        });

        req.write(objData);

        req.once('drain', () => {
            req.end();
        });
    });
});
