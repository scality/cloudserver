import assert from 'assert';
import http from 'http';
import https from 'https';

import conf from '../../../../../lib/Config';

const transport = conf.https ? https : http;
const ipAddress = process.env.IP ? process.env.IP : '127.0.0.1';
const hostname = process.env.AWS_ON_AIR ? 's3.amazonaws.com' :
    ipAddress;
const port = process.env.AWS_ON_AIR ? 80 : 8000;


const statusCode = {
    200: 200,
    NoSuchBucket: 404,
    BadRequest: 400,
    AccessForbidden: 403,
};

export default function methodRequest(params, callback) {
    const { method, bucket, objectKey, headers, code, headersResponse } =
        params;
    const options = {
        hostname,
        port,
        method,
        headers,
        path: objectKey ? `/${bucket}/${objectKey}` : `/${bucket}`,
        rejectUnauthorized: false,
    };
    const req = transport.request(options, res => {
        const body = [];
        res.on('data', chunk => {
            body.push(chunk);
        });
        res.on('error', err => {
            process.stdout.write('err receiving response');
            return callback(err);
        });
        res.on('end', () => {
            const total = body.join('');
            if (code) {
                const message = code === 200 ? '' : `<Code>${code}</Code>`;
                assert(total.indexOf(message) > -1, `Expected ${message}`);
                assert.deepEqual(res.statusCode, statusCode[code],
                `status code expected: ${statusCode[code]}`);
            }
            if (headersResponse) {
                Object.keys(headersResponse).forEach(key => {
                    assert.deepEqual(res.headers[key], headersResponse[key],
                      `error header: ${key}`);
                });
            } else {
            // if no headersResponse provided, should not have these headers
            // in the request
                ['access-control-allow-origin',
                'access-control-allow-methods',
                'access-control-allow-credentials',
                'vary'].forEach(key => {
                    assert.strictEqual(res.headers[key], undefined,
                        `Error: ${key} should not have value`);
                });
            }
            return callback();
        });
    });

    req.on('error', err => {
        process.stdout.write('err sending request');
        return callback(err);
    });
    req.end();
}
