const AWS = require('aws-sdk');
const uuid4 = require('uuid/v4');
const config = require('../config.json');
const { auth } = require('arsenal');
const http = require('http');
const https = require('https');
const assert = require('assert');
const logger = { info: msg => process.stdout.write(`${msg}\n`) };


function _createBucket(name, done) {
    const { transport, ipAddress, accessKey, secretKey } = config;
    const verbose = false;
    const options = {
        host: ipAddress,
        port: 8000,
        method: 'PUT',
        path: `/${name}/`,
        headers: {
            'x-amz-scal-server-side-encryption': 'AES256',
        },
        rejectUnauthorized: false,
    };
    logger.info(`Creating encrypted bucket ${name}`);
    const client = transport === 'https' ? https : http;
    const request = client.request(options, response => {
        if (verbose) {
            logger.info('response status code', {
                statusCode: response.statusCode,
            });
            logger.info('response headers', { headers: response.headers });
        }
        const body = [];
        response.setEncoding('utf8');
        response.on('data', chunk => body.push(chunk));
        response.on('end', () => {
            if (response.statusCode >= 200 && response.statusCode < 300) {
                logger.info('Success', {
                    statusCode: response.statusCode,
                    body: verbose ? body.join('') : undefined,
                });
                done(null);
            } else {
                done({
                    statusCode: response.statusCode,
                    body: body.join(''),
                });
            }
        });
    });
    auth.client.generateV4Headers(request, '', accessKey, secretKey, 's3');
    if (verbose) {
        logger.info('request headers', { headers: request._headers });
    }
    request.end();
}

function _buildS3() {
    const { transport, ipAddress, accessKey, secretKey } = config;
    AWS.config.update({
        endpoint: `${transport}://${ipAddress}:8000`,
        accessKeyId: accessKey,
        secretAccessKey: secretKey,
        sslEnabled: transport === 'https',
        s3ForcePathStyle: true,
    });
    return new AWS.S3();
}
const s3 = _buildS3();

describe('KMIP backed server-side encryption', () => {
    let bucketName;
    let objectName;

    beforeEach(() => {
        bucketName = uuid4();
        objectName = uuid4();
    });

    it('should create an encrypted bucket', done => {
        _createBucket(bucketName, err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should create an encrypted bucket and upload an object', done => {
        _createBucket(bucketName, err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            const params = {
                Bucket: bucketName,
                Key: objectName,
                Body: 'I am the best content ever',
            };
            s3.putObject(params, err => {
                assert.equal(err, null, 'Expected success, ' +
                `got error ${JSON.stringify(err)}`);
                done();
            });
        });
    });
});
