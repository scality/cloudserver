const crypto = require('crypto');
const http = require('http');
const stream = require('stream');
const url = require('url');

const SERVICE = 's3';
const REGION = 'us-east-1';
const EMPTY_STRING_HASH = crypto.createHash('sha256').digest('hex');

/**
 * Execute and sign HTTP requests with AWS signature v4 scheme
 *
 * The purpose of this class is primarily testing, where the various
 * functions used to generate the signing content can be overriden for
 * specific test needs, like altering signatures or hashes.
 *
 * It provides a writable stream interface like the request object
 * returned by http.request().
 */
class HttpRequestAuthV4 extends stream.Writable {
    /**
     * @constructor
     * @param {string} url - HTTP URL to the S3 server
     * @param {object} params - request parameters
     * @param {string} params.accessKey - AWS access key
     * @param {string} params.secretKey - AWS secret key
     * @param {string} [params.method="GET"] - HTTP method
     * @param {object} [params.headers] - HTTP request headers
     * example: {
     *     'connection': 'keep-alive',
     *     'content-length': 1000, // mandatory for PUT object requests
     *     'x-amz-content-sha256': '...' // streaming V4 encoding is used
     *                                   // if not provided
     * }
     * @param {function} callback - called when a response arrives:
     * callback(res) (see http.request())
     */
    constructor(url, params, callback) {
        super();
        this._url = url;
        this._accessKey = params.accessKey;
        this._secretKey = params.secretKey;
        this._httpParams = params;
        this._callback = callback;

        this._httpRequest = null;
        this._timestamp = null;
        this._signingKey = null;
        this._chunkedUpload = false;
        this._lastSignature = null;

        this.once('finish', () => {
            if (!this._httpRequest) {
                this._initiateRequest(false);
            }
            if (this._chunkedUpload) {
                this._httpRequest.end(this.constructChunkPayload(''));
            } else {
                this._httpRequest.end();
            }
        });
    }

    getCredentialScope() {
        const signingDate = this._timestamp.slice(0, 8);
        const credentialScope =
              `${signingDate}/${REGION}/${SERVICE}/aws4_request`;
        // console.log(`CREDENTIAL SCOPE: "${credentialScope}"`);
        return credentialScope;
    }

    getSigningKey() {
        const signingDate = this._timestamp.slice(0, 8);
        const dateKey = crypto.createHmac('sha256', `AWS4${this._secretKey}`)
              .update(signingDate, 'binary').digest();
        const dateRegionKey = crypto.createHmac('sha256', dateKey)
              .update(REGION, 'binary').digest();
        const dateRegionServiceKey = crypto.createHmac('sha256', dateRegionKey)
              .update(SERVICE, 'binary').digest();
        this._signingKey = crypto.createHmac('sha256', dateRegionServiceKey)
              .update('aws4_request', 'binary').digest();
    }

    createSignature(stringToSign) {
        if (!this._signingKey) {
            this.getSigningKey();
        }
        return crypto.createHmac('sha256', this._signingKey)
            .update(stringToSign).digest('hex');
    }

    getCanonicalRequest(urlObj, signedHeaders) {
        const method = this._httpParams.method || 'GET';
        const signedHeadersList = Object.keys(signedHeaders).sort();
        const qsParams = [];
        urlObj.searchParams.forEach((value, key) => {
            qsParams.push({ key, value });
        });
        const canonicalQueryString =
              qsParams
              .sort((a, b) => {
                  if (a.key !== b.key) {
                      return a.key < b.key ? -1 : 1;
                  }
                  return a.value < b.value ? -1 : 1;
              })
              .map(param => `${encodeURI(param.key)}=${encodeURI(param.value)}`)
              .join('&');
        const canonicalSignedHeaders = signedHeadersList
              .map(header => `${header}:${signedHeaders[header]}\n`)
              .join('');
        const canonicalRequest = [
            method,
            urlObj.pathname,
            canonicalQueryString,
            canonicalSignedHeaders,
            signedHeadersList.join(';'),
            signedHeaders['x-amz-content-sha256'],
        ].join('\n');

        // console.log(`CANONICAL REQUEST: "${canonicalRequest}"`);
        return canonicalRequest;
    }

    constructRequestStringToSign(canonicalReq) {
        const canonicalReqHash =
              crypto.createHash('sha256').update(canonicalReq).digest('hex');
        const stringToSign = `AWS4-HMAC-SHA256\n${this._timestamp}\n` +
              `${this.getCredentialScope()}\n${canonicalReqHash}`;
        // console.log(`STRING TO SIGN: "${stringToSign}"`);
        return stringToSign;
    }

    getAuthorizationSignature(urlObj, signedHeaders) {
        const canonicalRequest =
              this.getCanonicalRequest(urlObj, signedHeaders);
        this._lastSignature = this.createSignature(
            this.constructRequestStringToSign(canonicalRequest));
        return this._lastSignature;
    }

    getAuthorizationHeader(urlObj, signedHeaders) {
        const authorizationSignature =
              this.getAuthorizationSignature(urlObj, signedHeaders);
        const signedHeadersList = Object.keys(signedHeaders).sort();

        return ['AWS4-HMAC-SHA256',
                `Credential=${this._accessKey}/${this.getCredentialScope()},`,
                `SignedHeaders=${signedHeadersList.join(';')},`,
                `Signature=${authorizationSignature}`,
               ].join(' ');
    }

    constructChunkStringToSign(chunkData) {
        const currentChunkHash =
              crypto.createHash('sha256').update(chunkData.toString())
              .digest('hex');
        const stringToSign = `AWS4-HMAC-SHA256-PAYLOAD\n${this._timestamp}\n` +
              `${this.getCredentialScope()}\n${this._lastSignature}\n` +
              `${EMPTY_STRING_HASH}\n${currentChunkHash}`;
        // console.log(`CHUNK STRING TO SIGN: "${stringToSign}"`);
        return stringToSign;
    }

    getChunkSignature(chunkData) {
        const stringToSign = this.constructChunkStringToSign(chunkData);
        this._lastSignature = this.createSignature(stringToSign);
        return this._lastSignature;
    }

    constructChunkPayload(chunkData) {
        if (!this._chunkedUpload) {
            return chunkData;
        }
        const chunkSignature = this.getChunkSignature(chunkData);
        return [chunkData.length.toString(16),
                ';chunk-signature=',
                chunkSignature,
                '\r\n',
                chunkData,
                '\r\n',
               ].join('');
    }

    _constructRequest(hasDataToSend) {
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
        let contentLengthHeader;
        Object.keys(httpHeaders).forEach(header => {
            const lowerHeader = header.toLowerCase();
            if (lowerHeader === 'content-length') {
                contentLengthHeader = header;
            }
            if (!['connection',
                  'transfer-encoding'].includes(lowerHeader)) {
                signedHeaders[lowerHeader] = httpHeaders[header];
            }
        });
        if (!signedHeaders['x-amz-content-sha256']) {
            if (hasDataToSend) {
                signedHeaders['x-amz-content-sha256'] =
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD';
                signedHeaders['content-encoding'] = 'aws-chunked';
                this._chunkedUpload = true;
                if (contentLengthHeader !== undefined) {
                    signedHeaders['x-amz-decoded-content-length'] =
                        httpHeaders[contentLengthHeader];
                    delete signedHeaders['content-length'];
                    delete httpHeaders[contentLengthHeader];
                    httpHeaders['transfer-encoding'] = 'chunked';
                }
            } else {
                signedHeaders['x-amz-content-sha256'] = EMPTY_STRING_HASH;
            }
        }
        httpHeaders.Authorization =
            this.getAuthorizationHeader(urlObj, signedHeaders);

        return Object.assign(httpHeaders, signedHeaders);
    }

    _initiateRequest(hasDataToSend) {
        const httpParams = Object.assign({}, this._httpParams);
        httpParams.headers = this._constructRequest(hasDataToSend);
        this._httpRequest = http.request(this._url, httpParams, this._callback);
    }

    _write(chunk, encoding, callback) {
        if (!this._httpRequest) {
            this._initiateRequest(true);
        }
        const payload = this.constructChunkPayload(chunk);
        if (this._httpRequest.write(payload)) {
            return callback();
        }
        return this._httpRequest.once('drain', callback);
    }
}

module.exports = HttpRequestAuthV4;
