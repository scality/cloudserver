import { expect } from 'chai';
import async from 'async';
import crypto from 'crypto';
import { parseString } from 'xml2js';
import utils from '../../lib/utils.js';
import bucketPut from '../../lib/api/bucketPut.js';
import bucketDelete from '../../lib/api/bucketDelete.js';
import bucketHead from '../../lib/api/bucketHead.js';
import objectPut from '../../lib/api/objectPut.js';
import objectHead from '../../lib/api/objectHead.js';
import objectGet from '../../lib/api/objectGet.js';
import objectDelete from '../../lib/api/objectDelete.js';
import bucketGet from '../../lib/api/bucketGet.js';
import serviceGet from '../../lib/api/serviceGet.js';
import bucketPutACL from '../../lib/api/bucketPutACL.js';
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
        const bucketName = 'BucketName';
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

    it('should return an error if bucketname is invalid', (done) => {
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
        const bucketName = 'BucketName';
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
    it('should return an error if ACL set in header ' +
       'with an invalid group URI', (done) => {
        const bucketName = 'BucketName';
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
        const bucketName = 'BucketName';
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
        const bucketName = 'BucketName';
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
        const bucketName = 'BucketName';
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
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
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
        const bucketName = 'BucketName';
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
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
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

describe("bucketDelete API", () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'bucketName';
    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const testDeleteRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace
    };

    it('should return an error if the bucket is not empty', (done) => {
        const postBody = 'I am a body';
        const objectName = 'objectName';
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };

        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                () => {
                    bucketDelete(accessKey, metastore, testDeleteRequest,
                        (err) => {
                            expect(err).to.equal('BucketNotEmpty');
                            expect(metastore.users[accessKey]
                                .buckets).to.have.length.of(1);
                            expect(Object.keys(metastore.buckets))
                                .to.have.length.of(1);
                            done();
                        });
                });
        });
    });

    it('should delete a bucket', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            bucketDelete(accessKey, metastore, testDeleteRequest,
                (err, response) => {
                    expect(response).to
                        .equal('Bucket deleted permanently');
                    expect(metastore.users[accessKey].buckets)
                        .to.have.length.of(0);
                    expect(Object.keys(metastore.buckets))
                        .to.have.length.of(0);
                    done();
                });
        });
    });
});

describe('bucketHead API', () => {
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

    it('should return an error if the bucket does not exist', (done) => {
        const bucketName = 'BucketName';
        const testRequest = {
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketHead(accessKey, metastore, testRequest, (err) => {
            expect(err).to.equal('NoSuchBucket');
            done();
        });
    });

    it('should return an error if user is not authorized', (done) => {
        const bucketName = 'BucketName';
        const putAccessKey = 'accessKey2';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketPut(putAccessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketHead(accessKey, metastore, testRequest,
                    (err) => {
                        expect(err).to.equal('AccessDenied');
                        done();
                    });
            });
    });

    it('should return a success message if ' +
       'bucket exists and user is authorized', (done) => {
        const bucketName = 'BucketName';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketHead(accessKey, metastore, testRequest,
                    (err, result) => {
                        expect(result).to.equal(
                            'Bucket exists and user authorized -- 200');
                        done();
                    });
            });
    });
});

describe('objectPut API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });


    it('should return an error if the bucket does not exist', (done) => {
        const bucketName = 'BucketName';
        const postBody = 'I am a body';
        const testRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        objectPut(accessKey, datastore, metastore, testRequest,
            (err) => {
                expect(err).to.equal('NoSuchBucket');
                done();
            });
    });

    it('should return an error if user is not authorized', (done) => {
        const bucketName = 'BucketName';
        const postBody = 'I am a body';
        const putAccessKey = 'accessKey2';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
            post: postBody
        };

        bucketPut(putAccessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                    (err) => {
                        expect(err).to.equal('AccessDenied');
                        done();
                    });
            });
    });

    it('should return an error if Content MD-5 is invalid', (done) => {
        const bucketName = 'BucketName';
        const postBody = 'I am a body';
        const incorrectMD5 = 'asdfwelkjflkjslfjskj993ksjl';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                'content-md5': incorrectMD5
            },
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: `/${objectName}`,
            namespace: namespace,
            post: postBody
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err) => {
                        expect(err).to.equal('InvalidDigest');
                        done();
                    });
            });
    });

    it.skip('should return an error if datastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once services.putDataStore
        // includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it.skip('should return an error if metastore ' +
            'reports an error back', () => {
        // TODO: Test to be written once
        // services.metadataStoreObject includes an actual call to
        // datastore rather than just the in
        // memory adding of a key/value pair to the datastore
        // object
    });

    it('should successfully put an object', (done) => {
        const bucketName = 'BucketName';
        const postBody = 'I am a body';
        const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
        const objectUID = '84c130398c854348bcff8b715f793dc4';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: '/',
            namespace: namespace
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        expect(
                            metastore.buckets[bucketUID].keyMap[objectName])
                            .to.exist;
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['content-md5'])
                                .to.equal(correctMD5);
                        expect(datastore[objectUID]).to.equal('I am a body');
                        done();
                    });
            });
    });

    it('should successfully put an object with user metadata', (done) => {
        const bucketName = 'BucketName';
        const postBody = 'I am a body';
        const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
        const objectName = 'objectName';
        const testPutBucketRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace,
        };
        const testPutObjectRequest = {
            lowerCaseHeaders: {
                // Note that Node will collapse common headers into one
                // (e.g. "x-amz-meta-test: hi" and "x-amz-meta-test:
                // there" becomes "x-amz-meta-test: hi, there")
                // Here we are not going through an actual http
                // request so will not collapse properly.
                'x-amz-meta-test': 'some metadata',
                'x-amz-meta-test2': 'some more metadata',
                'x-amz-meta-test3': 'even more metadata',
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: postBody,
            calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]).to.exist;
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test'])
                                    .to.equal('some metadata');
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test2'])
                                    .to.equal('some more metadata');
                        expect(
                            metastore.buckets[bucketUID]
                                .keyMap[objectName]['x-amz-meta-test3'])
                                    .to.equal('even more metadata');
                        done();
                    });
            });
    });
});

describe('objectHead API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'BucketName';
    const postBody = 'I am a body';
    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const incorrectMD5 = 'fkjwelfjlslfksdfsdfsdfsdfsdfsdj';
    const objectName = 'objectName';
    const date = new Date();
    const laterDate = date.setMinutes(date.getMinutes() + 30);
    const earlierDate = date.setMinutes(date.getMinutes() - 30);
    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        lowerCaseHeaders: {
            'x-amz-meta-test': userMetadataValue
        },
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };


    it('should return NotModified if request header ' +
       'includes "if-modified-since" and object ' +
       'not modified since specified time', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-modified-since': laterDate
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectHead(accessKey, metastore, testGetRequest,
                            (err) => {
                                expect(err).to.equal('NotModified');
                                done();
                            });
                    });
            });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-unmodified-since" and object has ' +
       'been modified since specified time', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-unmodified-since': earlierDate
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };
        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                expect(err).to.equal('PreconditionFailed');
                                done();
                            });
                    });
            });
    });

    it('should return PreconditionFailed if request header ' +
       'includes "if-match" and Etag of object ' +
       'does not match specified Etag', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-match': incorrectMD5
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                expect(err).to.equal('PreconditionFailed');
                                done();
                            });
                    });
            });
    });

    it('should return NotModified if request header ' +
       'includes "if-none-match" and Etag of object does ' +
       'match specified Etag', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                'if-none-match': correctMD5
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err) => {
                                expect(err).to.equal('NotModified');
                                done();
                            });
                    });
            });
    });

    it('should get the object metadata', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectHead(accessKey, metastore,
                            testGetRequest, (err, success) => {
                                expect(success[userMetadataKey])
                                    .to.equal(userMetadataValue);
                                expect(success.Etag)
                                    .to.equal(correctMD5);
                                done();
                            });
                    });
            });
    });
});

describe('objectGet API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'BucketName';
    const postBody = 'I am a body';
    const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
    const objectName = 'objectName';
    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const userMetadataKey = 'x-amz-meta-test';
    const userMetadataValue = 'some metadata';
    const testPutObjectRequest = {
        lowerCaseHeaders: {
            'x-amz-meta-test': 'some metadata'
        },
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };

    it("should get the object metadata", (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest,
                            (err, result, responseMetaHeaders) => {
                                expect(responseMetaHeaders[userMetadataKey])
                                    .to.equal(userMetadataValue);
                                expect(responseMetaHeaders.Etag)
                                    .to.equal(correctMD5);
                                done();
                            });
                    });
            });
    });

    it('should get the object data', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectGet(accessKey, datastore, metastore,
                            testGetRequest, (err, result) => {
                                expect(result).to.equal(postBody);
                                done();
                            });
                    });
            });
    });

    it('should get the object data for large objects', (done) => {
        const testBigData = crypto.randomBytes(1000000);
        const correctBigMD5 =
            crypto.createHash('md5').update(testBigData).digest('hex');

        const testPutBigObjectRequest = {
            lowerCaseHeaders: {
                'x-amz-meta-test': 'some metadata'
            },
            url: `/${bucketName}/${objectName}`,
            namespace: namespace,
            post: testBigData,
            calculatedMD5: correctBigMD5
        };

        const testGetRequest = {
            lowerCaseHeaders: {},
            url: `/${bucketName}/${objectName}`,
            namespace: namespace
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutBigObjectRequest, (err, result) => {
                        expect(result).to.equal(correctBigMD5);
                        objectGet(accessKey, datastore,
                            metastore, testGetRequest, (err, result) => {
                                const resultmd5Hash =
                                    crypto.createHash('md5')
                                        .update(result).digest('hex');
                                expect(resultmd5Hash).to.equal(correctBigMD5);
                                done();
                            });
                    });
            });
    });
});


describe('objectDelete API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'bucketName';
    const testBucketPutRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const postBody = 'I am a body';
    const objectName = 'objectName';
    const testPutObjectRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace,
        post: postBody,
        calculatedMD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a'
    };
    const testDeleteRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName}`,
        namespace: namespace
    };

    it.skip('should set delete markers ' +
            'when versioning enabled', () => {
        // TODO
    });

    it('should delete an object', (done) => {
        bucketPut(accessKey, metastore, testBucketPutRequest, () => {
            objectPut(accessKey, datastore, metastore, testPutObjectRequest,
                () => {
                    objectDelete(accessKey, datastore, metastore,
                        testDeleteRequest, (err, response) => {
                            expect(response)
                                .to.equal('Object deleted permanently');
                            expect(Object.keys(datastore)
                                .length).to.equal(0);
                            done();
                        });
                });
        });
    });
});

describe('bucketGet API', () => {
    let metastore;
    let datastore;

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
        datastore = {};
    });

    const bucketName = 'BucketName';
    const postBody = 'I am a body';
    const prefix = 'sub';
    const delimiter = '/';
    const objectName1 = `${prefix}${delimiter}objectName1`;
    const objectName2 = `${prefix}${delimiter}objectName2`;

    const testPutBucketRequest = {
        lowerCaseHeaders: {},
        url: `/${bucketName}`,
        namespace: namespace,
    };
    const testPutObjectRequest1 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName1}`,
        namespace: namespace,
        post: postBody,
    };
    const testPutObjectRequest2 = {
        lowerCaseHeaders: {},
        url: `/${bucketName}/${objectName2}`,
        namespace: namespace,
        post: postBody
    };

    it('should return the name of the common prefix ' +
       'of common prefix objects if delimiter ' +
       'and prefix specified', (done) => {
        const commonPrefix = `${prefix}${delimiter}`;
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}?delimiter=\/&prefix=sub`,
            namespace: namespace,
            query: {
                delimiter: delimiter,
                prefix: prefix
            }
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.CommonPrefixes[0].Prefix[0])
                .to.equal(commonPrefix);
            done();
        });
    });

    it('should return list of all objects if ' +
       'no delimiter specified', (done) => {
        const testGetRequest = {
            lowerCaseHeaders: {
                host: '/'
            },
            url: `/${bucketName}`,
            namespace: namespace,
            query: {}
        };


        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testPutBucketRequest, next);
            },
            function waterfall2(success, next) {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest1, next);
            },
            function waterfall3(result, next) {
                objectPut(accessKey, datastore,
                    metastore, testPutObjectRequest2, next);
            },
            function waterfall4(result, next) {
                bucketGet(accessKey, metastore,
                    testGetRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListBucketResult.Contents[0].Key[0])
                .to.equal(objectName1);
            expect(result.ListBucketResult.Contents[1].Key[0])
                .to.equal(objectName2);
            done();
        });
    });
});

describe('serviceGet API', () => {
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

    it('should return the list of buckets owned by the user', (done) => {
        const bucketName1 = 'BucketName1';
        const bucketName2 = 'BucketName2';
        const bucketName3 = 'BucketName3';
        const testbucketPutRequest1 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName1}.s3.amazonaws.com`}
        };
        const testbucketPutRequest2 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName2}.s3.amazonaws.com`}
        };
        const testbucketPutRequest3 = {
            lowerCaseHeaders: {},
            url: '/',
            namespace: namespace,
            headers: {host: `${bucketName3}.s3.amazonaws.com`}
        };
        const serviceGetRequest = {
            lowerCaseHeaders: {host: 's3.amazonaws.com'},
            url: '/',
        };

        async.waterfall([
            function waterfall1(next) {
                bucketPut(accessKey, metastore, testbucketPutRequest1, next);
            },
            function waterfall2(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest2, next);
            },
            function waterfall3(result, next) {
                bucketPut(accessKey, metastore, testbucketPutRequest3, next);
            },
            function waterfall4(result, next) {
                serviceGet(accessKey, metastore, serviceGetRequest, next);
            },
            function waterfall4(result, next) {
                parseString(result, next);
            }
        ],
        function waterfallFinal(err, result) {
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket)
                .to.have.length.of(3);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[0].Name[0])
                .to.equal(bucketName1);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[1].Name[0])
                .to.equal(bucketName2);
            expect(result.ListAllMyBucketsResult.Buckets[0].Bucket[2].Name[0])
                .to.equal(bucketName3);
            done();
        });
    });
});

describe('putBucketACL API', () => {
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

    it("should parse a grantheader", function testGrantHeader() {
        const grantRead =
            'uri="http://acs.amazonaws.com/groups/s3/LogDelivery", ' +
            'emailAddress="test@testing.com", ' +
            'emailAddress="test2@testly.com", ' +
            'id="sdfsdfsfwwiieohefs"';
        const grantReadHeader =
            utils.parseGrant(grantRead, 'read');
        const firstIdentifier = grantReadHeader[0].identifier;
        expect(firstIdentifier).to.
            equal('http://acs.amazonaws.com/groups/s3/LogDelivery');
        const secondIdentifier = grantReadHeader[1].identifier;
        expect(secondIdentifier).to.equal('test@testing.com');
        const thirdIdentifier = grantReadHeader[2].identifier;
        expect(thirdIdentifier).to.equal('test2@testly.com');
        const fourthIdentifier = grantReadHeader[3].identifier;
        expect(fourthIdentifier).to.equal('sdfsdfsfwwiieohefs');
        const fourthType = grantReadHeader[3].userIDType;
        expect(fourthType).to.equal('id');
        const grantType = grantReadHeader[3].grantType;
        expect(grantType).to.equal('read');
    });

    it('should return an error if invalid canned ACL provided', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'not-a-valid-option'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'not-a-valid-option'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal(
                            'InvalidArgument');
                        done();
                    });
            });
    });

    it('should set a canned public-read-write ACL', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.be.null;
                        expect(metastore.buckets[bucketUID]
                            .acl.Canned).to.equal('public-read-write');
                        done();
                    });
            });
    });

    it('should set a canned public-read ACL followed by'
        + 'a canned authenticated-read ACL', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const testACLRequest2 = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.be.null;
                        expect(metastore.buckets[bucketUID]
                            .acl.Canned).to.equal('public-read');
                        bucketPutACL(accessKey, metastore, testACLRequest2,
                            (err) => {
                                expect(err).to.be.null;
                                expect(metastore.buckets[bucketUID]
                                    .acl.Canned).to.equal('authenticated-read');
                                done();
                            });
                    });
            });
    });

    it('should set a canned private ACL ' +
        'followed by a log-delivery-write ACL', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const testACLRequest2 = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write'
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write'
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.be.null;
                        expect(metastore.buckets[bucketUID]
                            .acl.Canned).to.equal('private');
                        bucketPutACL(accessKey, metastore, testACLRequest2,
                            (err) => {
                                expect(err).to.be.null;
                                expect(metastore.buckets[bucketUID]
                                    .acl.Canned).to.equal('log-delivery-write');
                                done();
                            });
                    });
            });
    });

    it('should set ACLs provided in request headers', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
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
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
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
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
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

    it('should return an error if invalid email ' +
        'provided in ACL header request', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentEmail@sampling.com"',
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentEmail@sampling.com"',
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('UnresolvableGrantByEmailAddress');
                        done();
                    });
            });
    });

    it('should set ACLs provided in request body', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                    'BucketOwnerCanonicalUserID</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                        'BucketOwnerCanonicalUserID</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        '<URI>http://acs.amazonaws.com/groups/' +
                        'global/AllUsers</URI>' +
                      '</Grantee>' +
                      '<Permission>READ</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        '<URI>http://acs.amazonaws.com/groups/s3/Log' +
                        'Delivery</URI>' +
                      '</Grantee>' +
                      '<Permission>WRITE</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>sampleaccount1@sampling.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>f30716ab7115dcb44a5ef76e9d74b8e20567f63' +
                        'TestAccountCanonicalUserID</ID>' +
                      '</Grantee>' +
                      '<Permission>READ_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };
        const bucketUID = '84d4cad3cdb50ad21b6c1660a92627b3';
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.be.null;
                        expect(metastore.buckets[bucketUID]
                            .acl.Canned).to.equal('');
                        expect(metastore.buckets[bucketUID].acl.FULL_CONTROL[0])
                            .to.equal(
                                '852b113e7a2f25102679df27bb0ae12b3f85be6' +
                                'BucketOwnerCanonicalUserID');
                        expect(metastore.buckets[bucketUID].acl.READ[0])
                                    .to.equal('http://acs.amazonaws.com/' +
                                            'groups/global/AllUsers');
                        expect(metastore.buckets[bucketUID].acl.WRITE[0])
                            .to.equal('http://acs.amazonaws.com/' +
                                    'groups/s3/LogDelivery');
                        expect(metastore.buckets[bucketUID].acl.WRITE_ACP[0])
                            .to.equal(canonicalIDforSample1);
                        expect(metastore.buckets[bucketUID].acl.READ_ACP[0])
                                .to.equal('f30716ab7115dcb44a5e' +
                                'f76e9d74b8e20567f63' +
                                'TestAccountCanonicalUserID');
                        done();
                    });
            });
    });

    it('should return an error if invalid email ' +
    'address provided in ACLs set out in request body', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                    'BucketOwnerCanonicalUserID</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('UnresolvableGrantByEmailAddress');
                        done();
                    });
            });
    });

    it('should return an error if xml provided does not match s3 ' +
    'scheme for setting ACLs', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            // XML below uses the term "PowerGrant" instead of
            // "Grant" which is part of the s3 xml shceme for ACLs
            // so an error should be returned
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                    'BucketOwnerCanonicalUserID</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<PowerGrant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</PowerGrant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('MalformedACLError');
                        done();
                    });
            });
    });

    it('should return an error if malformed xml provided', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            // XML below fails to close each container properly
            // so an error should be returned
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                    'BucketOwnerCanonicalUserID</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '<Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com</EmailAddress>' +
                      '<Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '<Grant>' +
                  '<AccessControlList>' +
                '<AccessControlPolicy>'},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('MalformedXML');
                        done();
                    });
            });
    });

    it('should return an error if invalid group ' +
    'uri provided in ACLs set out in request body', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            // URI in grant below is not valid group URI for s3
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6' +
                    'BucketOwnerCanonicalUserID</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                  '<Grant>' +
                    '<Grantee xsi:type="Group">' +
                      '<URI>http://acs.amazonaws.com/groups/' +
                      'global/NOTAVALIDGROUP</URI>' +
                    '</Grantee>' +
                    '<Permission>READ</Permission>' +
                  '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('InvalidArgument');
                        done();
                    });
            });
    });

    it('should return an error if invalid group uri' +
        'provided in ACL header request', (done) => {
        const bucketName = 'BucketName';
        const testBucketPutRequest = {
            lowerCaseHeaders: {},
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            url: '/',
            namespace: namespace
        };
        const testACLRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            headers: {
                host: `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/?acl',
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testBucketPutRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                bucketPutACL(accessKey, metastore, testACLRequest,
                    (err) => {
                        expect(err).to.equal('InvalidArgument');
                        done();
                    });
            });
    });
});
