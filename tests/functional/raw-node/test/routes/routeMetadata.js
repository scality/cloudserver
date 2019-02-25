const assert = require('assert');

const { makeRequest } = require('../../utils/makeRequest');

const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';

const metadataAuthCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

function makeMetadataRequest(params, callback) {
    const { method, headers, authCredentials,
        requestBody, queryObj, path } = params;
    const options = {
        authCredentials,
        hostname: ipAddress,
        port: 8000,
        method,
        headers,
        path,
        requestBody,
        jsonResponse: true,
        queryObj,
    };
    makeRequest(options, callback);
}

describe('check routes metadata', () => {
    it('should return not implemented error', done => {
        makeMetadataRequest({
            method: 'GET',
            authCredentials: metadataAuthCredentials,
            path: '/_/metadata/listbuckets/1',
        }, err => {
            assert(err);
            assert.strictEqual(err.code, 'NotImplemented');
            return done();
        });
    });
});
