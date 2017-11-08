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

const aclTests = [
    // CEPH: test_website_private_bucket_list_private_index_blockederrordoc
    {
        it: 'should return 403 if private bucket index and error documents',
        bucketACL: 'private',
        objects: { index: 'private', error: 'private' },
        html: '403-access-denied',
    },
    // CEPH: test_website_public_bucket_list_private_index_blockederrordoc
    {
        it: 'should return 403 if public bucket - private index - public ' +
        'error documents',
        bucketACL: 'public',
        objects: { index: 'private', error: 'private' },
        html: '403-access-denied',
    },
    {
        it: 'should return index doc if private bucket - public index - ' +
        'public error documents',
        bucketACL: 'private',
        objects: { index: 'public-read', error: 'private' },
        html: 'index-user',
    },
    {
        it: 'should return index doc if public bucket - public index - ' +
        'private error documents',
        bucketACL: 'public',
        objects: { index: 'public-read', error: 'private' },
        html: 'index-user',
    },
    {
        it: 'should return index doc if private bucket - public index - ' +
        'public error documents',
        bucketACL: 'private',
        objects: { index: 'public-read', error: 'public-read' },
        html: 'index-user',
    },
    {
        it: 'should return index doc if public bucket - public index - ' +
        'public error documents',
        bucketACL: 'public',
        objects: { index: 'public-read', error: 'public-read' },
        html: 'index-user',
    },

    {
        it: 'should return error doc if private bucket - without index - ' +
        'public error documents',
        bucketACL: 'private',
        objects: { error: 'public-read' },
        html: 'error-user',
    },
    {
        it: 'should return 404 if public bucket - without index - ' +
        'public error documents',
        bucketACL: 'public',
        objects: { error: 'public-read' },
        html: 'error-user-404',
    },

    // CEPH: test_website_private_bucket_list_empty_blockederrordoc
    {
        it: 'should return 403 if private bucket - without index - ' +
        'private error documents',
        bucketACL: 'private',
        objects: { error: 'private' },
        html: '403-access-denied',
    },

    // CEPH: test_website_public_bucket_list_empty_blockederrordoc
    {
        it: 'should return 404 if public bucket - without index - ' +
        'private error documents',
        bucketACL: 'public',
        objects: { error: 'private' },
        html: '404-not-found',
    },

    // CEPH: test_website_public_bucket_list_empty_missingerrordoc
    {
        it: 'should return 404 if public bucket - without index - ' +
        'without error documents',
        bucketACL: 'public',
        objects: { },
        html: '404-not-found',
    },
    {
        it: 'should return 403 if private bucket - without index - ' +
        'without error documents',
        bucketACL: 'private',
        objects: { },
        html: '403-access-denied',
    },

];

describe('User visits bucket website endpoint with ACL', () => {
    aclTests.forEach(test => {
        aclEquivalent[test.bucketACL].forEach(bucketACL => {
            describe(`with existing bucket with ${bucketACL} acl`, () => {
                beforeEach(done => {
                    WebsiteConfigTester.createPutBucketWebsite(s3, bucket,
                      bucketACL, test.objects, done);
                });
                afterEach(done => {
                    WebsiteConfigTester.deleteObjectsThenBucket(s3, bucket,
                    test.objects, err => {
                        if (process.env.AWS_ON_AIR) {
                            // Give some time for AWS to finish deleting
                            // object and buckets before starting next test
                            setTimeout(() => done(err), 10000);
                        } else {
                            done(err);
                        }
                    });
                });

                it(`${test.it} with no auth credentials sent`, done => {
                    WebsiteConfigTester.checkHTML({
                        method: 'GET',
                        url: endpoint,
                        requestType: test.html,
                    }, done);
                });

                it(`${test.it} even with invalid auth credentials`, done => {
                    WebsiteConfigTester.checkHTML({
                        auth: 'invalid credentials',
                        method: 'GET',
                        url: endpoint,
                        requestType: test.html,
                    }, done);
                });

                it(`${test.it} even with valid auth credentials`, done => {
                    WebsiteConfigTester.checkHTML({
                        auth: 'valid credentials',
                        method: 'GET',
                        url: endpoint,
                        requestType: test.html,
                    }, done);
                });
            });
        });
    });
});
