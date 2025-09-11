const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

describe('bucketPutVersioning API - Content-MD5 validation', () => {
    const versioningXML = '<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>';

    before(() => cleanup());
    beforeEach(done => {
        // Create bucket first
        bucketPut(authInfo, testBucketPutRequest, log, done);
    });
    afterEach(() => cleanup());

    it('should not return an error when Content-MD5 header is missing', done => {
        const testVersioningRequest = {
            bucketName,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: versioningXML,
            url: '/?versioning',
            query: { versioning: '' },
            actionImplicitDenies: false,
        };

        bucketPutVersioning(authInfo, testVersioningRequest, log, err => {
            assert.ifError(err);
            done();
        });
    });

    it('should return BadDigest error when Content-MD5 header mismatches', done => {
        const testVersioningRequest = {
            bucketName,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'content-md5': '+5yj3kZsXledyKr18eaUDg==', // incorrect MD5
            },
            post: versioningXML,
            url: '/?versioning',
            query: { versioning: '' },
            actionImplicitDenies: false,
        };

        bucketPutVersioning(authInfo, testVersioningRequest, log, err => {
            assert.deepStrictEqual(err, errors.BadDigest);
            done();
        });
    });

    it('should not return an error when Content-MD5 header matches', done => {
        const testVersioningRequest = {
            bucketName,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'content-md5': '8qj8HSeDu3APPMQZVG06WQ==', // correct MD5
            },
            post: versioningXML,
            url: '/?versioning',
            query: { versioning: '' },
            actionImplicitDenies: false,
        };

        bucketPutVersioning(authInfo, testVersioningRequest, log, err => {
            assert.ifError(err);
            done();
        });
    });
});
