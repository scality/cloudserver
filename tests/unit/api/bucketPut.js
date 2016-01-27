import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import constants from '../../../constants';
import metadata from '../metadataswitch';
import DummyRequestLogger from '../helpers';

const log = new DummyRequestLogger();


const accessKey = 'accessKey1';
const namespace = 'default';
const splitter = constants.splitter;
const usersBucket = constants.usersBucket;
const bucketName = 'bucketname';
const testRequest = {
    bucketName,
    namespace,
    lowerCaseHeaders: {},
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` }
};

describe('bucketPut API', () => {
    beforeEach((done) => {
        metadata.deleteBucket(bucketName, log, ()=> {
            metadata.deleteBucket(usersBucket, log, () => {
                done();
            });
        });
    });

    after((done) => {
        metadata.deleteBucket(bucketName, log, ()=> {
            metadata.deleteBucket(usersBucket, log, () => {
                done();
            });
        });
    });

    it('should return an error if bucket already exists', (done) => {
        const otherAccessKey = 'accessKey2';
        bucketPut(accessKey,  testRequest, log, () => {
            bucketPut(otherAccessKey,  testRequest, log,
                    (err) => {
                        assert.strictEqual(err, 'BucketAlreadyExists');
                        done();
                    });
        });
    });

    it('should return an error if malformed xml ' +
       'is provided in request.post', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            post: 'malformedxml'
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'MalformedXML');
            done();
        });
    });

    it('should return an error if xml which does ' +
       'not conform to s3 docs is provided in request.post', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            post: '<Hello></Hello>'
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'MalformedXML');
            done();
        });
    });

    it('should return an error if LocationConstraint ' +
       'specified is not valid', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            post:
                '<CreateBucketConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                '<LocationConstraint>invalidLocation</LocationConstraint>'
                + '</CreateBucketConfiguration>'
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'InvalidLocationConstraint');
            done();
        });
    });

    it('should create a bucket', (done) => {
        bucketPut(accessKey,  testRequest, log, (err, success) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(success, 'Bucket created');
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.name, bucketName);
                assert.strictEqual(md.owner, accessKey);
                const prefix = `${accessKey}${splitter}`;
                metadata.listObject(usersBucket, prefix,
                    null, null, null, log, (err, listResponse) => {
                        assert.strictEqual(listResponse.Contents[0].key,
                            `${accessKey}${splitter}${bucketName}`);
                        done();
                    });
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid group URI', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/',
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'InvalidArgument');
            metadata.getBucket(bucketName, log, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid canned ACL', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                'x-amz-acl': 'not-valid-option',
            },
            url: '/',
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'InvalidArgument');
            metadata.getBucket(bucketName, log, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid email address', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                'x-amz-grant-read':
                    'emailaddress="fake@faking.com"',
            },
            url: '/',
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, 'UnresolvableGrantByEmailAddress');
            metadata.getBucket(bucketName, log, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should set a canned ACL while creating bucket' +
        ' if option set out in header', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                'x-amz-acl':
                    'public-read',
            },
            url: '/',
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, null);
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(err, null);
                assert.strictEqual(md.acl.Canned, 'public-read');
                done();
            });
        });
    });

    it('should set specific ACL grants while creating bucket' +
        ' if options set out in header', (done) => {
        const testRequest = {
            bucketName,
            namespace,
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-write':
                    'uri="http://acs.amazonaws.com/groups/global/AllUsers"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: '/',
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        bucketPut(accessKey,  testRequest, log, (err) => {
            assert.strictEqual(err, null, 'Error creating bucket');
            metadata.getBucket(bucketName, log, (err, md) => {
                assert.strictEqual(md.acl.READ[0],
                    'http://acs.amazonaws.com/groups/s3/LogDelivery');
                assert.strictEqual(md.acl.WRITE[0],
                    'http://acs.amazonaws.com/groups/global/AllUsers');
                assert(md.acl.FULL_CONTROL.indexOf(canonicalIDforSample1) > -1);
                assert(md.acl.FULL_CONTROL.indexOf(canonicalIDforSample2) > -1);
                assert(md.acl.READ_ACP.indexOf(canonicalIDforSample1) > -1);
                assert(md.acl.WRITE_ACP.indexOf(canonicalIDforSample2) > -1);
                done();
            });
        });
    });

    it('should prevent anonymous user from accessing ' +
        'putBucket API', (done) => {
        bucketPut('http://acs.amazonaws.com/groups/global/AllUsers',
             testRequest, log,
                (err) => {
                    assert.strictEqual(err, 'AccessDenied');
                });
        done();
    });
});
