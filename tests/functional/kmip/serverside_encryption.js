const AWS = require('aws-sdk');
const { v4: uuidv4 } = require('uuid');
const config = require('../config.json');
const { auth } = require('arsenal');
const http = require('http');
const https = require('https');
const assert = require('assert');
const logger = { info: msg => process.stdout.write(`${msg}\n`) };
const async = require('async');


function _createBucket(name, encrypt, done) {
    const { transport, ipAddress, accessKey, secretKey } = config;
    const verbose = false;
    const options = {
        host: ipAddress,
        port: 8000,
        method: 'PUT',
        path: `/${name}/`,
        rejectUnauthorized: false,
    };

    if (encrypt) {
        Object.assign(options, {
            headers: {
                'x-amz-scal-server-side-encryption': 'AES256',
            },
        });
    }

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

function _putObject(bucketName, objectName, encrypt, cb) {
    const params = {
        Bucket: bucketName,
        Key: objectName,
        Body: 'I am the best content ever',
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.putObject(params, cb);
}

function _copyObject(sourceBucket, sourceObject, targetBucket, targetObject,
    encrypt, cb) {
    const params = {
        Bucket: targetBucket,
        CopySource: `/${sourceBucket}/${sourceObject}`,
        Key: targetObject,
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.copyObject(params, cb);
}

function _initiateMultipartUpload(bucketName, objectName, encrypt, cb) {
    const params = {
        Bucket: bucketName,
        Key: objectName,
    };

    if (encrypt) {
        Object.assign(params, {
            ServerSideEncryption: 'AES256',
        });
    }

    s3.createMultipartUpload(params, cb);
}

describe('KMIP backed server-side encryption', () => {
    let bucketName;
    let objectName;

    beforeEach(() => {
        bucketName = uuidv4();
        objectName = uuidv4();
    });

    it('should create an encrypted bucket', done => {
        _createBucket(bucketName, true, err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should create an encrypted bucket and upload an object', done => {
        async.waterfall([
            next => _createBucket(bucketName, true, err => next(err)),
            next => _putObject(bucketName, objectName, false, err => next(err)),
        ], err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should allow object PUT with SSE header in encrypted bucket', done => {
        async.waterfall([
            next => _createBucket(bucketName, true, err => next(err)),
            next => _putObject(bucketName, objectName, true, err => next(err)),
        ], err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should allow object copy with SSE header in encrypted bucket', done => {
        async.waterfall([
            next => _createBucket(bucketName, false, err => next(err)),
            next => _putObject(bucketName, objectName, false, err => next(err)),
            next => _createBucket(`${bucketName}2`, true, err => next(err)),
            next => _copyObject(bucketName, objectName, `${bucketName}2`,
                `${objectName}2`, true, err => next(err)),
        ], err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });

    it('should allow creating mpu with SSE header ' +
        'in encrypted bucket', done => {
        async.waterfall([
            next => _createBucket(bucketName, true, err => next(err)),
            next => _initiateMultipartUpload(bucketName, objectName,
                true, err => next(err)),
        ], err => {
            assert.equal(err, null, 'Expected success, ' +
            `got error ${JSON.stringify(err)}`);
            done();
        });
    });
});
