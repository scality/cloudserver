import { expect } from 'chai';
import objectPut from '../../../lib/api/objectPut';
import bucketPut from '../../../lib/api/bucketPut';
import objectPutACL from '../../../lib/api/objectPutACL';

const accessKey = 'accessKey1';
const namespace = 'default';

describe('putObjectACL API', () => {
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

    it('should return an error if invalid canned ACL provided', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'invalid-option'
            },
            headers: {
                'x-amz-acl': 'invalid-option'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err).to.equal('InvalidArgument');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set a canned public-read-write ACL', (done) => {
        const bucketName = 'bucketname';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read-write'
            },
            headers: {
                'x-amz-acl': 'public-read-write'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err).to.equal(undefined);
                                expect(metastore
                                        .buckets[bucketUID]
                                        .keyMap.objectName
                                        .acl.Canned)
                                        .to.equal('public-read-write');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set a canned public-read ACL followed by'
        + ' a canned authenticated-read ACL', (done) => {
        const bucketName = 'bucketname';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest1 = {
            lowerCaseHeaders: {
                'x-amz-acl': 'public-read'
            },
            headers: {
                'x-amz-acl': 'public-read'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        const testObjACLRequest2 = {
            lowerCaseHeaders: {
                'x-amz-acl': 'authenticated-read'
            },
            headers: {
                'x-amz-acl': 'authenticated-read'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest1,
                            (err) => {
                                expect(err).to.equal(undefined);
                                expect(metastore
                                        .buckets[bucketUID]
                                        .keyMap.objectName
                                        .acl.Canned)
                                        .to.equal('public-read');
                                objectPutACL(accessKey, metastore,
                                    testObjACLRequest2,
                                    (err) => {
                                        expect(err).to.equal(undefined);
                                        expect(metastore
                                                .buckets[bucketUID]
                                                .keyMap.objectName
                                                .acl.Canned)
                                                .to.equal('authenticated-read');
                                        done();
                                    }
                                );
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set ACLs provided in request headers', (done) => {
        const bucketName = 'bucketname';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
        const canonicalIDforSample2 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read':
                    'uri="http://acs.amazonaws.com/groups/s3/LogDelivery"',
                'x-amz-grant-read-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be"',
                'x-amz-grant-write-acp':
                    'id="79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err).to.equal(undefined);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.READ[0])
                                    .to.equal('http://acs.amazonaws.com/' +
                                        'groups/s3/LogDelivery');
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.FULL_CONTROL[0]
                                    .indexOf(canonicalIDforSample1))
                                    .to.be.above(-1);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.FULL_CONTROL[1]
                                    .indexOf(canonicalIDforSample2))
                                    .to.be.above(-1);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.READ_ACP[0]
                                    .indexOf(canonicalIDforSample1))
                                    .to.be.above(-1);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.WRITE_ACP[0]
                                    .indexOf(canonicalIDforSample2))
                                    .to.be.above(-1);
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid email ' +
        'provided in ACL header request', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentemail@sampling.com"'
            },
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentemail@sampling.com"'
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                .to.equal('UnresolvableGrantByEmailAddress');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should set ACLs provided in request body', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
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
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>sampleaccount1@sampling.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>f30716ab7115dcb44a5ef76e9d74b8e20567f63</ID>' +
                      '</Grantee>' +
                      '<Permission>READ_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const canonicalIDforSample1 =
            '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to.equal(undefined);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.FULL_CONTROL[0])
                                    .to.equal(
                                        '852b113e7a2f25102679df27bb' +
                                        '0ae12b3f85be6');
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.READ[0])
                                    .to.equal('http://acs.amazonaws.com/' +
                                            'groups/global/AllUsers');
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.WRITE_ACP[0])
                                    .to.equal(canonicalIDforSample1);
                                expect(metastore.
                                    buckets[bucketUID]
                                    .keyMap.objectName
                                    .acl.READ_ACP[0])
                                    .to.equal('f30716ab7115dcb44a5e' +
                                    'f76e9d74b8e20567f63');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should ignore if WRITE ACL permission is ' +
        'provided in request body', (done) => {
        const bucketName = 'bucketname';
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        '<URI>http://acs.amazonaws.com/groups/' +
                        'global/AllUsers</URI>' +
                      '</Grantee>' +
                      '<Permission>WRITE</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };

        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err).to.be.undefined;
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.Canned)
                                    .to.equal('');
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.FULL_CONTROL[0])
                                    .to.equal('852b113e7a2f2510267' +
                                        '9df27bb0ae12b3f85be6');
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.WRITE).to.be.undefined;
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.READ[0]).to.be.undefined;
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.WRITE_ACP[0])
                                    .to.be.undefined;
                                expect(metastore.buckets[bucketUID].keyMap
                                    .objectName.acl.READ_ACP[0])
                                    .to.be.undefined;
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid email ' +
    'address provided in ACLs set out in request body', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to
                                    .equal('UnresolvableGrantByEmailAddress');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if xml provided does not match s3 ' +
    'scheme for setting ACLs', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<PowerGrant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</PowerGrant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to
                                    .equal('MalformedACLError');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if malformed xml provided', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com' +
                        '</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '<Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to
                                    .equal('MalformedXML');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid group ' +
    'uri provided in ACLs set out in request body', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {},
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>852b113e7a2f25102679df27bb0ae12b3f85be6</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                    '<Grantee xsi:type="Group">' +
                      '<URI>http://acs.amazonaws.com/groups/' +
                      'global/NOTAVALIDGROUP</URI>' +
                    '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '<Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>'},
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to
                                    .equal('MalformedXML');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });

    it('should return an error if invalid group uri ' +
        'provided in ACL header request', (done) => {
        const bucketName = 'bucketname';
        const postBody = 'I am a body';
        const correctMD5 = 'vnR+tLdVF79rPPfF+7YvOg==';
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
            namespace,
            post: postBody,
            calculatedMD5: 'vnR+tLdVF79rPPfF+7YvOg=='
        };

        const testObjACLRequest = {
            lowerCaseHeaders: {
                host: `s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            namespace: namespace,
            headers: {
                host: `s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            query: {
                acl: ''
            }
        };


        bucketPut(accessKey, metastore, testPutBucketRequest,
            (err, success) => {
                expect(success).to.equal('Bucket created');
                objectPut(accessKey, datastore, metastore,
                    testPutObjectRequest, (err, result) => {
                        expect(result).to.equal(correctMD5);
                        objectPutACL(accessKey, metastore, testObjACLRequest,
                            (err) => {
                                expect(err)
                                    .to
                                    .equal('InvalidArgument');
                                done();
                            }
                        );
                    }
                );
            }
        );
    });
});
