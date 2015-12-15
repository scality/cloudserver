import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import metadata from '../metadataswitch';
import utils from '../../../lib/utils';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('bucketPut API', () => {
    let metastore;
    const bucketName = 'bucketname';
    const testBucketUID = utils.getResourceUID(namespace, bucketName);

    beforeEach((done) => {
        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": []
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {}
        };
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    after((done) => {
        metadata.deleteBucket(testBucketUID, ()=> {
            done();
        });
    });

    it('should return an error if bucket already exists', (done) => {
        const otherAccessKey = 'accessKey2';
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, () => {
            bucketPut(otherAccessKey, metastore, testRequest,
                    (err) => {
                        assert.strictEqual(err, 'BucketAlreadyExists');
                        done();
                    });
        });
    });

    it('should return an error if bucketname is invalid' +
    ' because bucketname is too short', (done) => {
        const tooShortBucketName = 'hi';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${tooShortBucketName}`,
            namespace: namespace,
            post: ''
        };

        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'InvalidBucketName');
            done();
        });
    });

    it('should return an error if bucketname is invalid' +
    ' because bucketname has capital letters', (done) => {
        const hasCapsBucketName = 'noSHOUTING';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${hasCapsBucketName}`,
            namespace: namespace,
            post: ''
        };

        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'InvalidBucketName');
            done();
        });
    });

    it('should return an error if malformed xml ' +
       'is provided in request.post', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/test1',
            namespace: namespace,
            post: 'malformedxml'
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'MalformedXML');
            done();
        });
    });


    it('should return an error if xml which does ' +
       'not conform to s3 docs is provided in request.post', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/test1',
            namespace: namespace,
            post: '<Hello></Hello>'
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'MalformedXML');
            done();
        });
    });

    it('should return an error if LocationConstraint ' +
       'specified is not valid', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/test1',
            namespace: namespace,
            post:
                '<CreateBucketConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                '<LocationConstraint>invalidLocation</LocationConstraint>'
                + '</CreateBucketConfiguration>'
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'InvalidLocationConstraint');
            done();
        });
    });

    it('should create a bucket using ' +
       'bucket name provided in path', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            namespace: namespace,
            post: ''
        };

        bucketPut(accessKey, metastore, testRequest, (err, success) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(success, 'Bucket created');
            metadata.getBucket(testBucketUID, (err, md) => {
                assert.strictEqual(md.name, bucketName);
                assert.strictEqual(md.owner, accessKey);
                assert.strictEqual(metastore
                    .users[accessKey].buckets.length, 1);
                done();
            });
        });
    });

    it('should create a bucket using bucket ' +
       'name provided in host', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, (err, success) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(success, 'Bucket created');
            metadata.getBucket(testBucketUID, (err, md) => {
                assert.strictEqual(md.name, bucketName);
                assert.strictEqual(md.owner, accessKey);
                assert.strictEqual(metastore.users[accessKey].
                    buckets.length, 1);
                done();
            });
        });
    });

    it('should not create duplicate buckets', (done) => {
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            namespace: namespace,
            post: ''
        };
        const differentAccount = 'accessKey2';

        bucketPut(accessKey, metastore, testRequest, () => {
            bucketPut(differentAccount, metastore, testRequest, (err) => {
                assert.strictEqual(err, 'BucketAlreadyExists');
                metadata.getBucket(testBucketUID, (err, md) => {
                    assert.strictEqual(md.name, bucketName);
                    // The bucket that is actually created
                    // should be the one put by accessKey
                    // rather than differentAccount
                    assert.strictEqual(md.owner, accessKey);
                    assert.strictEqual(metastore.users[accessKey]
                        .buckets.length, 1);
                    done();
                });
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid group URI', (done) => {
        const testRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'InvalidArgument');
            metadata.getBucket(testBucketUID, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid canned ACL', (done) => {
        const testRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'not-valid-option',
            },
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'InvalidArgument');
            metadata.getBucket(testBucketUID, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid email address', (done) => {
        const testRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-read':
                    'emailaddress="fake@faking.com"',
            },
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, 'UnresolvableGrantByEmailAddress');
            metadata.getBucket(testBucketUID, (err) => {
                assert.strictEqual(err, 'NoSuchBucket');
                done();
            });
        });
    });

    it('should set a canned ACL while creating bucket' +
        ' if option set out in header', (done) => {
        const testRequest = {
            lowerCaseHeaders: {
                'x-amz-acl':
                    'public-read',
            },
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, null);
            metadata.getBucket(testBucketUID, (err, md) => {
                assert.strictEqual(err, null);
                assert.strictEqual(md.acl.Canned, 'public-read');
                done();
            });
        });
    });

    it('should set specific ACL grants while creating bucket' +
        ' if options set out in header', (done) => {
        const testRequest = {
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
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        bucketPut(accessKey, metastore, testRequest, (err) => {
            assert.strictEqual(err, null, 'Error creating bucket');
            metadata.getBucket(testBucketUID, (err, md) => {
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
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            namespace: namespace,
            post: ''
        };
        bucketPut('http://acs.amazonaws.com/groups/global/AllUsers',
            metastore, testRequest,
                (err) => {
                    assert.strictEqual(err, 'AccessDenied');
                });
        done();
    });
});
