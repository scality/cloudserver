import { S3 } from 'aws-sdk';

import conf from '../../../../../lib/Config';
import getConfig from '../support/config';
import { WebsiteConfigTester } from '../../lib/utility/website-util';

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

const transport = conf.https ? 'https' : 'http';
const bucket = process.env.AWS_ON_AIR ? 'awsbucketwebsitetester' :
    'bucketwebsitetester';
const hostname = `${bucket}.s3-website-us-east-1.amazonaws.com`;
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
                      test.objects, done);
                });

                it(`${test.it}`, done => {
                    WebsiteConfigTester.checkHTML(endpoint, test.html,
                    null, null, done);
                });
            });
        });
    });
});
