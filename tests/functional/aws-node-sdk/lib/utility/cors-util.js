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
    301: 301, // website redirect
    403: 403, // website AccessDenied error
    404: 404, // website NoSuchBucket error
    AccessForbidden: 403,
    AccessDenied: 403,
    BadRequest: 400,
    InvalidAccessKeyId: 403,
    InvalidArgument: 400,
    NoSuchBucket: 404,
};

export default function methodRequest(params, callback) {
    const { method, bucket, objectKey, query, headers, code,
        headersResponse, headersOmitted, isWebsite } = params;
    const websiteHostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;

    const options = {
        port,
        method,
        headers,
        rejectUnauthorized: false,
    };
    if (isWebsite) {
        options.hostname = websiteHostname;
        options.path = objectKey ? `/${objectKey}` : '/';
    } else {
        options.hostname = hostname;
        options.path = objectKey ? `/${bucket}/${objectKey}` : `/${bucket}`;
    }
    if (query) {
        options.path = `${options.path}?${query}`;
    }
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
                const message = isNaN(parseInt(code, 10)) ?
                    `<Code>${code}</Code>` : '';
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
            if (headersOmitted) {
                headersOmitted.forEach(key => {
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

// for testing needs usually only need one rule, if support for more CORS
// rules is desired, can refactor
export function generateCorsParams(bucket, params) {
    const corsParams = {
        Bucket: bucket,
        CORSConfiguration: {
            CORSRules: [],
        },
    };
    const rule = {};
    Object.keys(params).forEach(key => {
        rule[`${key.charAt(0).toUpperCase()}${key.slice(1)}`] = params[key];
    });
    corsParams.CORSConfiguration.CORSRules[0] = rule;

    return corsParams;
}
