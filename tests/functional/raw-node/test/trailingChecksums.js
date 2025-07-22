const assert = require('assert');
const async = require('async');
const { makeS3Request } = require('../utils/makeRequest');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');

const bucket = 'testunsupportedchecksumsbucket';
const objectKey = 'key';
const objData = Buffer.alloc(1024, 'a');
// note this is not the correct checksum in objDataWithTrailingChecksum
const objDataWithTrailingChecksum =
    '10\r\n0123456789abcdef\r\n' + '10\r\n0123456789abcdef\r\n' + '0\r\nx-amz-checksum-crc64nvme:YeIDuLa7tU0=\r\n';
const objDataWithoutTrailingChecksum = '0123456789abcdef0123456789abcdef';

const config = require('../../config.json');
const authCredentials = {
    accessKey: config.accessKey,
    secretKey: config.secretKey,
};

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('trailing checksum requests:', () => {
    before(done => {
        makeS3Request(
            {
                method: 'PUT',
                authCredentials,
                bucket,
            },
            err => {
                assert.ifError(err);
                done();
            }
        );
    });

    after(done => {
        async.series(
            [
                next =>
                    makeS3Request(
                        {
                            method: 'DELETE',
                            authCredentials,
                            bucket,
                            objectKey,
                        },
                        next
                    ),
                next =>
                    makeS3Request(
                        {
                            method: 'DELETE',
                            authCredentials,
                            bucket,
                        },
                        next
                    ),
            ],
            err => {
                assert.ifError(err);
                done();
            }
        );
    });

    it('should accept unsigned trailing checksum', done => {
        const req = new HttpRequestAuthV4(
            `http://localhost:8000/${bucket}/${objectKey}`,
            Object.assign(
                {
                    method: 'PUT',
                    headers: {
                        'content-length': objDataWithTrailingChecksum.length,
                        'x-amz-decoded-content-length': objDataWithoutTrailingChecksum.length,
                        'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                        'x-amz-trailer': 'x-amz-checksum-crc64nvme',
                    },
                },
                authCredentials
            ),
            res => {
                assert.strictEqual(res.statusCode, 200);
                res.on('data', () => {});
                res.on('end', done);
            }
        );

        req.on('error', err => {
            assert.ifError(err);
        });

        req.write(objDataWithTrailingChecksum);

        req.once('drain', () => {
            req.end();
        });
    });

    it('should have correct object content for unsigned trailing checksum', done => {
        makeS3Request(
            {
                method: 'GET',
                authCredentials,
                bucket,
                objectKey,
            },
            (err, res) => {
                assert.ifError(err);
                assert.strictEqual(res.statusCode, 200);
                // check that the object data is the input stripped of the trailing checksum
                assert.strictEqual(res.body, objDataWithoutTrailingChecksum);
                return done();
            }
        );
    });

    itSkipIfAWS('should respond with BadRequest for signed trailing checksum', done => {
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
