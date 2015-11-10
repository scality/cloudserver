import { expect } from 'chai';
import utils from '../../../lib/utils';
import bucketPut from '../../../lib/api/bucketPut';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('bucketPut API', () => {
    let metastore;

    beforeEach(() => {
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
    });


    it('should return an error if bucket already exists', (done) => {
        const bucketName = 'bucketname';
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
                        expect(err).to.equal('BucketAlreadyExists');
                        done();
                    });
        });
    });

    it('should return an error if bucketname is invalid' +
    'because bucketname is too short', (done) => {
        const tooShortBucketName = 'hi';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${tooShortBucketName}`,
            namespace: namespace,
            post: ''
        };

        bucketPut(accessKey, metastore, testRequest, (err) => {
            expect(err).to.equal('InvalidBucketName');
            done();
        });
    });

    it('should return an error if bucketname is invalid' +
    'because bucketname has capital letters', (done) => {
        const hasCapsBucketName = 'noSHOUTING';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${hasCapsBucketName}`,
            namespace: namespace,
            post: ''
        };

        bucketPut(accessKey, metastore, testRequest, (err) => {
            expect(err).to.equal('InvalidBucketName');
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
            expect(err).to.equal('MalformedXML');
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
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.equal('MalformedXML');
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
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.equal('InvalidLocationConstraint');
                done();
            });
    });

    it('should create a bucket using ' +
       'bucket name provided in path', (done) => {
        const bucketName = 'test1';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            namespace: namespace,
            post: ''
        };
        const testBucketUID =
            utils.getResourceUID(testRequest.namespace, bucketName);

        bucketPut(accessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                expect(metastore.buckets[testBucketUID].name)
                    .to.equal(bucketName);
                expect(metastore.buckets[testBucketUID].owner)
                    .to.equal(accessKey);
                expect(metastore.users[accessKey].buckets)
                    .to.have.length.of.at.least(1);
                done();
            });
    });

    it('should create a bucket using bucket ' +
       'name provided in host', (done) => {
        const bucketName = 'bucketname';
        const testRequest = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        const testBucketUID =
            utils.getResourceUID(testRequest.namespace, bucketName);
        bucketPut(accessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                expect(metastore.buckets[testBucketUID].name)
                    .to.equal(bucketName);
                expect(metastore.buckets[testBucketUID].owner)
                    .to.equal(accessKey);
                expect(metastore.users[accessKey].buckets)
                    .to.have.length.of.at.least(1);
                done();
            });
    });

    it('should not create duplicate buckets', (done) => {
        const bucketName = 'test1';
        const testRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}`,
            namespace: namespace,
            post: ''
        };
        const testBucketUID =
            utils.getResourceUID(testRequest.namespace, bucketName);

        bucketPut(accessKey, metastore, testRequest,
            () => {
                bucketPut(accessKey, metastore, testRequest, (err) => {
                    expect(err).to.equal('BucketAlreadyExists');
                    expect(metastore.buckets[testBucketUID].name)
                        .to.equal(bucketName);
                    expect(metastore.buckets[testBucketUID].owner)
                        .to.equal(accessKey);
                    expect(metastore.users[accessKey].buckets)
                        .to.have.length.of(1);
                    expect(Object.keys(metastore.buckets))
                        .to.have.length.of(1);
                    done();
                });
            });
    });

    it('should return an error if ACL set in header ' +
       'with an invalid group URI', (done) => {
        const bucketName = 'bucketname';
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
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.equal('InvalidArgument');
                done();
            });
    });
    it('should return an error if ACL set in header ' +
       'with an invalid canned ACL', (done) => {
        const bucketName = 'bucketname';
        const testRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'not-valid-option',
            },
            url: '/',
            namespace: namespace,
            post: '',
            headers: {host: `${bucketName}.s3.amazonaws.com`}
        };
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.equal('InvalidArgument');
                done();
            });
    });
    it('should return an error if ACL set in header ' +
       'with an invalid email address', (done) => {
        const bucketName = 'bucketname';
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
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.equal('UnresolvableGrantByEmailAddress');
                done();
            });
    });
    it('should set a canned ACL while creating bucket' +
        ' if option set out in header', (done) => {
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.be.null;
                expect(metastore.buckets[bucketUID]
                    .acl.Canned).to.equal('public-read');
                done();
            });
    });
    it('should set specific ACL grants while creating bucket' +
        ' if options set out in header', (done) => {
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        bucketPut(accessKey, metastore, testRequest,
            (err) => {
                expect(err).to.be.null;
                expect(metastore.buckets[bucketUID].acl.READ[0])
                    .to.equal('http://acs.amazonaws.com/' +
                        'groups/s3/LogDelivery');
                expect(metastore.buckets[bucketUID].acl.WRITE[0])
                    .to.equal('http://acs.amazonaws.com/' +
                            'groups/global/AllUsers');
                expect(metastore.buckets[bucketUID].acl.FULL_CONTROL
                    .indexOf(canonicalIDforSample1)).to.be.above(-1);
                expect(metastore.buckets[bucketUID].acl.FULL_CONTROL
                    .indexOf(canonicalIDforSample2)).to.be.above(-1);
                expect(metastore.buckets[bucketUID].acl.READ_ACP
                    .indexOf(canonicalIDforSample1)).to.be.above(-1);
                expect(metastore.buckets[bucketUID].acl.WRITE_ACP
                    .indexOf(canonicalIDforSample2)).to.be.above(-1);
                done();
            });
    });
});
