const { S3 } = require('aws-sdk');

const conf = require('../../../../../lib/Config').config;
const getConfig = require('../support/config');
const { WebsiteConfigTester } = require('../../lib/utility/website-util');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

// Note: To run these tests locally, you may need to edit the machine's
// /etc/hosts file to include the following line:
// `127.0.0.1 bucketwebsitetester.s3-website-us-east-1.amazonaws.com`

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = process.env.S3_END_TO_END ?
    `${bucket}.s3-website-us-east-1.scality.com` :
    `${bucket}.s3-website-us-east-1.amazonaws.com`;
const endpoint = process.env.AWS_ON_AIR ? `${transport}://${hostname}` :
    `${transport}://${hostname}:8000`;

const aclEquivalent = {
    public: ['public-read-write', 'public-read'],
    private: ['private', 'authenticated-read'],
};

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

                it(`${test.it} with no auth credentials sent`, done => {
                    const result = test.result;
                    WebsiteConfigTester.makeHeadRequest(undefined, endpoint,
                        headersACL[result].status,
                        headersACL[result].expectedHeaders, done);
                });

                it(`${test.it} even with invalid auth credentials`, done => {
                    const result = test.result;
                    WebsiteConfigTester.makeHeadRequest('invalid credentials',
                        endpoint, headersACL[result].status,
                        headersACL[result].expectedHeaders, done);
                });

                it(`${test.it} even with valid auth credentials`, done => {
                    const result = test.result;
                    WebsiteConfigTester.makeHeadRequest('valid credentials',
                        endpoint, headersACL[result].status,
                        headersACL[result].expectedHeaders, done);
                });
            });
        });
    });
});
