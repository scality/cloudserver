const url = require('url');
const qs = require('qs');
const AWS = require('aws-sdk');
const werelogs = require('werelogs');
const { constructStringToSignV2 } = require('arsenal').auth.client;

const logger = new werelogs.Logger('GcpSigner');

function genQueryObject(uri) {
    const queryString = url.parse(uri).query;
    return qs.parse(queryString);
}

const GcpSigner = AWS.util.inherit(AWS.Signers.RequestSigner, {
    constructor: function GcpSigner(request) {
        AWS.Signers.RequestSigner.call(this, request);
    },

    addAuthorization: function addAuthorization(credentials, date) {
        if (!this.request.headers['presigned-expires']) {
            this.request.headers['x-goog-date'] = AWS.util.date.rfc822(date);
        }

        const signature =
            this.sign(credentials.secretAccessKey, this.stringToSign());
        const auth = `GOOG1 ${credentials.accessKeyId}: ${signature}`;

        this.request.headers.Authorization = auth;
    },

    stringToSign: function stringToSign() {
        const requestObject = {
            url: this.request.path,
            method: this.request.method,
            host: this.request.endpoint.host,
            headers: this.request.headers,
            query: genQueryObject(this.request.path) || {},
        };
        const data = Object.assign({}, this.request.headers);
        return constructStringToSignV2(requestObject, data, logger, 'GCP');
    },

    sign: function sign(secret, string) {
        return AWS.util.crypto.hmac(secret, string, 'base64', 'sha1');
    },
});

module.exports = GcpSigner;
