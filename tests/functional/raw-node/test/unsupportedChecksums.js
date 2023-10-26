const assert = require('assert');
const { makeS3Request } = require('../utils/makeRequest');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');

const bucket = 'testunsupportedchecksumsbucket';
const objectKey = 'key';
const objData = Buffer.alloc(1024, 'a');

const authCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('unsupported checksum requests:', () => {
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
        makeS3Request({
            method: 'DELETE',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
        });
    });

    itSkipIfAWS('should respond with BadRequest for trailing checksum', done => {
        const req = new HttpRequestAuthV4(
            `http://localhost:8000/${bucket}/${objectKey}`,
            Object.assign(
                {
                    method: 'PUT',
                    headers: {
                        'content-length': objData.length,
                        'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                        'x-amz-trailer': 'x-amz-checksum-sha256',
                    },
                },
                authCredentials
            ),
            res => {
                assert.strictEqual(res.statusCode, 400);
                res.on('data', () => {});
                res.on('end', done);
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
