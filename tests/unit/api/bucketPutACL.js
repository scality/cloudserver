import { expect } from 'chai';
import utils from '../../../lib/utils.js';
import bucketPut from '../../../lib/api/bucketPut.js';
import bucketPutACL from '../../../lib/api/bucketPutACL.js';

const accessKey = 'accessKey1';
const namespace = 'default';

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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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

        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';

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
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';

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
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';

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
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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
        const bucketUID = '911b9ca7dbfbe2b280a70ef0d2c2fb22';
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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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
        const bucketName = 'bucketname';
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
