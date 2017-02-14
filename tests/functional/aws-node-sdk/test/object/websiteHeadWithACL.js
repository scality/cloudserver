import http from 'http';
import https from 'https';
import assert from 'assert';

import { S3 } from 'aws-sdk';

import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;

const aclEquivalent = {
    public: ['public-read-write', 'public-read'],
    private: ['private', 'authenticated-read'],
};

function makeHeadRequest(expectedStatusCode, expectedHeaders, cb) {
    const options = {
        hostname,
        port: process.env.AWS_ON_AIR ? 80 : 8000,
        method: 'HEAD',
        rejectUnauthorized: false,
    };
    const module = conf.https ? https : http;
    const req = module.request(options, res => {
        const body = [];
        res.on('data', chunk => {
            body.push(chunk);
        });
        res.on('error', err => {
            process.stdout.write('err on post response');
            return cb(err);
        });
        res.on('end', () => {
            // body should be empty
            assert.deepStrictEqual(body, []);
            assert.strictEqual(res.statusCode, expectedStatusCode);
            const headers = Object.keys(expectedHeaders);
            for (let i = 0; i < headers.length; i++) {
                assert.strictEqual(res.headers[headers[i]],
                    expectedHeaders[headers[i]]);
            }
            return cb();
        });
    });
    req.on('error', err => {
        process.stdout.write('err from post request');
        return cb(err);
    });
    req.end();
}

const headersACL = {
    accessDenied: {
        status: 403,
        expectedHeaders: {
            'x-amz-error-code': 'AccessDenied',
            'x-amz-error-message': 'Access Denied',
        },
    },
    noSuchKey: {
        status: 404,
        expectedHeaders: {
            'x-amz-error-code': 'NoSuchKey',
            'x-amz-error-message': 'The specified key does not exist.',
        },
    },
    index: {
        status: 200,
        expectedHeaders: {
            etag: '"95a589c37a2df74b062fb4d5a6f64197"',
        },
    },
};

const aclTests = [
    {
        it: 'should return 403 if private bucket index and error documents',
        bucketACL: 'private',
        objects: { index: 'private', error: 'private' },
        result: 'accessDenied',
    },
    {
        it: 'should return 403 if public bucket - private index - public ' +
        'error documents',
        bucketACL: 'public',
        objects: { index: 'private', error: 'private' },
        result: 'accessDenied',
    },
    {
        it: 'should return 200 if private bucket - public index - ' +
        'public error documents',
        bucketACL: 'private',
        objects: { index: 'public-read', error: 'private' },
        result: 'index',
    },
    {
        it: 'should return 200 if public bucket - public index - ' +
        'private error documents',
        bucketACL: 'public',
        objects: { index: 'public-read', error: 'private' },
        result: 'index',
    },
    {
        it: 'should return 200 if private bucket - public index - ' +
        'public error documents',
        bucketACL: 'private',
        objects: { index: 'public-read', error: 'public-read' },
        result: 'index',
    },
    {
        it: 'should return 200 if public bucket - public index - ' +
        'public error documents',
        bucketACL: 'public',
        objects: { index: 'public-read', error: 'public-read' },
        result: 'index',
    },

    {
        it: 'should return 403 AccessDenied if private bucket - ' +
        'without index - public error documents',
        bucketACL: 'private',
        objects: { error: 'public-read' },
        result: 'accessDenied',
    },
    {
        it: 'should return 404 if public bucket - without index - ' +
        'public error documents',
        bucketACL: 'public',
        objects: { error: 'public-read' },
        result: 'noSuchKey',
    },

    {
        it: 'should return 403 if private bucket - without index - ' +
        'private error documents',
        bucketACL: 'private',
        objects: { error: 'private' },
        result: 'accessDenied',
    },

    {
        it: 'should return 404 if public bucket - without index - ' +
        'private error documents',
        bucketACL: 'public',
        objects: { error: 'private' },
        result: 'noSuchKey',
    },

    {
        it: 'should return 404 if public bucket - without index - ' +
        'without error documents',
        bucketACL: 'public',
        objects: { },
        result: 'noSuchKey',
    },
    {
        it: 'should return 403 if private bucket - without index - ' +
        'without error documents',
        bucketACL: 'private',
        objects: { },
        result: 'accessDenied',
    },
];

describe('Head request on bucket website endpoint with ACL', () => {
    aclTests.forEach(test => {
        aclEquivalent[test.bucketACL].forEach(bucketACL => {
            describe(`with existing bucket with ${bucketACL} acl`, () => {
                beforeEach(done => {
                    WebsiteConfigTester.createPutBucketWebsite(s3, bucket,
                      bucketACL, test.objects, done);
                });
                afterEach(done => {
                    WebsiteConfigTester.deleteObjectsThenBucket(s3, bucket,
                      test.objects, done);
                });

                it(`${test.it}`, done => {
                    const result = test.result;
                    makeHeadRequest(headersACL[result].status,
                      headersACL[result].expectedHeaders, done);
                });
            });
        });
    });
});
