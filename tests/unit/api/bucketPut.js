import { errors } from 'arsenal';
import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import { cleanup, DummyRequestLogger, makeAuthInfo } from '../helpers';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const namespace = 'default';
const splitter = constants.splitter;
const usersBucket = constants.usersBucket;
const bucketName = 'bucketname';
const locationConstraint = 'us-west-1';
const testRequest = {
    bucketName,
    namespace,
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};

describe('bucketPut API', () => {
    beforeEach(() => {
        cleanup();
    });

    it('should return an error if bucket already exists', done => {
        const otherAuthInfo = makeAuthInfo('accessKey2');
        bucketPut(authInfo, testRequest, locationConstraint, log, () => {
            bucketPut(otherAuthInfo, testRequest, locationConstraint,
                log, err => {
                    assert.deepStrictEqual(err, errors.BucketAlreadyExists);
                    done();
                });
        });
    });

    it('should create a bucket', done => {
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            if (err) {
                return done(new Error(err));
            }
            return metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.getName(), bucketName);
                assert.strictEqual(md.getOwner(), canonicalID);
                const prefix = `${canonicalID}${splitter}`;
                metadata.listObject(usersBucket, { prefix },
                    log, (err, listResponse) => {
                        assert.strictEqual(listResponse.Contents[0].key,
                            `${canonicalID}${splitter}${bucketName}`);
                        done();
                    });
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid group URI', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid canned ACL', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'not-valid-option',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid email address', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-read':
                    'emailaddress="fake@faking.com"',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.deepStrictEqual(err, errors.UnresolvableGrantByEmailAddress);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('should set a canned ACL while creating bucket' +
        ' if option set out in header', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl':
                    'public-read',
            },
            url: '/',
            post: '',
        };
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.strictEqual(err, undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(err, null);
                assert.strictEqual(md.getAcl().Canned, 'public-read');
                done();
            });
        });
    });

    it('should set specific ACL grants while creating bucket' +
        ' if options set out in header', done => {
        const testRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.logId}`,
                'x-amz-grant-write': `uri=${constants.publicId}`,
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: '/',
            post: '',
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        bucketPut(authInfo, testRequest, locationConstraint, log, err => {
            assert.strictEqual(err, undefined, 'Error creating bucket');
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.getAcl().READ[0], constants.logId);
                assert.strictEqual(md.getAcl().WRITE[0], constants.publicId);
                assert(md.getAcl()
                       .FULL_CONTROL.indexOf(canonicalIDforSample1) > -1);
                assert(md.getAcl()
                       .FULL_CONTROL.indexOf(canonicalIDforSample2) > -1);
                assert(md.getAcl()
                       .READ_ACP.indexOf(canonicalIDforSample1) > -1);
                assert(md.getAcl()
                       .WRITE_ACP.indexOf(canonicalIDforSample2) > -1);
                done();
            });
        });
    });

    it('should prevent anonymous user from accessing putBucket API', done => {
        const publicAuthInfo = makeAuthInfo(constants.publicId);
        bucketPut(publicAuthInfo, testRequest, locationConstraint, log, err => {
            assert.deepStrictEqual(err, errors.AccessDenied);
        });
        done();
    });
});
