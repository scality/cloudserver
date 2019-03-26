const assert = require('assert');
const { errors } = require('arsenal');

const aclUtils = require('../../../lib/utilities/aclUtils');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const constants = require('../../../constants');
const metadata = require('../metadataswitch');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};
const canonicalIDforSample1 =
    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be';
const canonicalIDforSample2 =
    '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2bf';

describe('putBucketACL API', () => {
    beforeAll(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    test('should parse a grantheader', () => {
        const grantRead =
            `uri=${constants.logId}, ` +
            'emailAddress="test@testing.com", ' +
            'emailAddress="test2@testly.com", ' +
            'id="sdfsdfsfwwiieohefs"';
        const grantReadHeader = aclUtils.parseGrant(grantRead, 'read');
        const firstIdentifier = grantReadHeader[0].identifier;
        expect(firstIdentifier).toBe(constants.logId);
        const secondIdentifier = grantReadHeader[1].identifier;
        expect(secondIdentifier).toBe('test@testing.com');
        const thirdIdentifier = grantReadHeader[2].identifier;
        expect(thirdIdentifier).toBe('test2@testly.com');
        const fourthIdentifier = grantReadHeader[3].identifier;
        expect(fourthIdentifier).toBe('sdfsdfsfwwiieohefs');
        const fourthType = grantReadHeader[3].userIDType;
        expect(fourthType).toBe('id');
        const grantType = grantReadHeader[3].grantType;
        expect(grantType).toBe('read');
    });

    test('should return an error if invalid canned ACL provided', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'not-a-valid-option',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    test('should set a canned public-read-write ACL', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read-write',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().Canned).toBe('public-read-write');
                done();
            });
        });
    });

    test('should set a canned public-read ACL followed by '
        + 'a canned authenticated-read ACL', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'public-read',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        const testACLRequest2 = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'authenticated-read',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().Canned).toBe('public-read');
                bucketPutACL(authInfo, testACLRequest2, log, err => {
                    expect(err).toBe(undefined);
                    metadata.getBucket(bucketName, log, (err, md) => {
                        expect(md.getAcl().Canned).toBe('authenticated-read');
                        done();
                    });
                });
            });
        });
    });

    test('should set a canned private ACL ' +
        'followed by a log-delivery-write ACL', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'private',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        const testACLRequest2 = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-acl': 'log-delivery-write',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().Canned).toBe('private');
                bucketPutACL(authInfo, testACLRequest2, log, err => {
                    expect(err).toBe(undefined);
                    metadata.getBucket(bucketName, log, (err, md) => {
                        expect(md.getAcl().Canned).toBe('log-delivery-write');
                        done();
                    });
                });
            });
        });
    });

    test('should set ACLs provided in request headers', done => {
        const testACLRequest = {
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
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2be',
                'x-amz-grant-write-acp':
                    'id=79a59df900b949e55d96a1e698fbacedfd6e09d98eac' +
                    'f8f8d5218e7cd47ef2bf',
            },
            url: '/?acl',
            query: { acl: '' },
        };
        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().WRITE[0]).toBe(constants.publicId);
                expect(md.getAcl().FULL_CONTROL
                    .indexOf(canonicalIDforSample1) > -1).toBeTruthy();
                expect(md.getAcl().FULL_CONTROL
                    .indexOf(canonicalIDforSample2) > -1).toBeTruthy();
                expect(md.getAcl().READ_ACP
                    .indexOf(canonicalIDforSample1) > -1).toBeTruthy();
                expect(md.getAcl().WRITE_ACP
                    .indexOf(canonicalIDforSample2) > -1).toBeTruthy();
                done();
            });
        });
    });

    test('should return an error if invalid grantee user ID ' +
    'provided in ACL header request', done => {
        // Canonical ID should be a 64-digit hex string
        const invalidCanonicalID = 'id="invalid_id"';
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control': invalidCanonicalID,
            },
            url: '/?acl',
            query: { acl: '' },
        };

        return bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    test('should return an error if invalid email ' +
        'provided in ACL header request', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"' +
                    ',emailaddress="nonexistentEmail@sampling.com"',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.UnresolvableGrantByEmailAddress);
            done();
        });
    });

    test('should set ACLs provided in request body', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>79a59df900b949e55d96a1e698fbaced' +
                        'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        `<URI>${constants.publicId}</URI>` +
                      '</Grantee>' +
                      '<Permission>READ</Permission>' +
                    '</Grant>' +
                    '<Grant>' +
                      '<Grantee xsi:type="Group">' +
                        `<URI>${constants.logId}</URI>` +
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
                        '<ID>79a59df900b949e55d96a1e698fbacedfd' +
                        '6e09d98eacf8f8d5218e7cd47ef2bf</ID>' +
                      '</Grantee>' +
                      '<Permission>READ_ACP</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().Canned).toBe('');
                expect(md.getAcl().FULL_CONTROL[0]).toBe(canonicalIDforSample1);
                expect(md.getAcl().READ[0]).toBe(constants.publicId);
                expect(md.getAcl().WRITE[0]).toBe(constants.logId);
                expect(md.getAcl().WRITE_ACP[0]).toBe(canonicalIDforSample1);
                expect(md.getAcl().READ_ACP[0]).toBe(canonicalIDforSample2);
                done();
            });
        });
    });

    test('should set ACLs with an empty AccessControlList section', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList></AccessControlList>' +
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            expect(err).toBe(undefined);
            metadata.getBucket(bucketName, log, (err, md) => {
                expect(md.getAcl().Canned).toBe('');
                expect(md.getAcl().FULL_CONTROL.length).toBe(0);
                expect(md.getAcl().READ.length).toBe(0);
                expect(md.getAcl().WRITE.length).toBe(0);
                expect(md.getAcl().WRITE_ACP.length).toBe(0);
                expect(md.getAcl().READ_ACP.length).toBe(0);
                done();
            });
        });
    });

    test(
        'should not be able to set ACLs without AccessControlList section',
        done => {
            const testACLRequest = {
                bucketName,
                namespace,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: '<AccessControlPolicy xmlns=' +
                        '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                      '<Owner>' +
                        '<ID>79a59df900b949e55d96a1e698fbaced' +
                        'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Owner>' +
                    '</AccessControlPolicy>',
                url: '/?acl',
                query: { acl: '' },
            };

            bucketPutACL(authInfo, testACLRequest, log, err => {
                assert.deepStrictEqual(err, errors.MalformedACLError);
                done();
            });
        }
    );

    test('should return an error if multiple AccessControlList section', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>79a59df900b949e55d96a1e698fbaced' +
                        'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>79a59df900b949e55d96a1e698fbaced' +
                        'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>READ</Permission>' +
                    '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.MalformedACLError);
            done();
        });
    });

    test('should return an error if invalid grantee user ID ' +
    'provided in ACL request body', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                  '<Grant>' +
                    '<Grantee xsi:type="CanonicalUser">' +
                      '<ID>invalid_id</ID>' +
                    '</Grantee>' +
                    '<Permission>READ_ACP</Permission>' +
                  '</Grant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        return bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    test('should return an error if invalid email ' +
    'address provided in ACLs set out in request body', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
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
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };
        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.UnresolvableGrantByEmailAddress);
            done();
        });
    });

    test('should return an error if xml provided does not match s3 '
       + 'scheme for setting ACLs', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            /** XML below uses the term "PowerGrant" instead of
            * "Grant" which is part of the s3 xml scheme for ACLs
            * so an error should be returned
            */
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
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
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.MalformedACLError);
            done();
        });
    });


    test('should return an error if xml provided does not match s3 '
       + 'scheme for setting ACLs using multiple Grant section', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            /** XML below uses the term "PowerGrant" instead of
            * "Grant" which is part of the s3 xml scheme for ACLs
            * so an error should be returned
            */
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                    '<DisplayName>OwnerDisplayName</DisplayName>' +
                  '</Owner>' +
                  '<AccessControlList>' +
                    '<Grant>' +
                      '<Grantee xsi:type="CanonicalUser">' +
                        '<ID>79a59df900b949e55d96a1e698fbaced' +
                        'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
                        '<DisplayName>OwnerDisplayName</DisplayName>' +
                      '</Grantee>' +
                      '<Permission>FULL_CONTROL</Permission>' +
                    '</Grant>' +
                    '<PowerGrant>' +
                      '<Grantee xsi:type="AmazonCustomerByEmail">' +
                        '<EmailAddress>xyz@amazon.com</EmailAddress>' +
                      '</Grantee>' +
                      '<Permission>WRITE_ACP</Permission>' +
                    '</PowerGrant>' +
                  '</AccessControlList>' +
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.MalformedACLError);
            done();
        });
    });

    test('should return an error if malformed xml provided', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            // XML below fails to close each container properly
            // so an error should be returned
            post: {
                '<AccessControlPolicy xmlns':
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
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
                '<AccessControlPolicy>',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.MalformedXML);
            done();
        });
    });

    test('should return an error if invalid group ' +
    'uri provided in ACLs set out in request body', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            // URI in grant below is not valid group URI for s3
            post: '<AccessControlPolicy xmlns=' +
                    '"http://s3.amazonaws.com/doc/2006-03-01/">' +
                  '<Owner>' +
                    '<ID>79a59df900b949e55d96a1e698fbaced' +
                    'fd6e09d98eacf8f8d5218e7cd47ef2be</ID>' +
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
                '</AccessControlPolicy>',
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });

    test('should return an error if invalid group uri' +
        'provided in ACL header request', done => {
        const testACLRequest = {
            bucketName,
            namespace,
            headers: {
                'host': `${bucketName}.s3.amazonaws.com`,
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/' +
                    'global/NOTAVALIDGROUP"',
            },
            url: '/?acl',
            query: { acl: '' },
        };

        bucketPutACL(authInfo, testACLRequest, log, err => {
            assert.deepStrictEqual(err, errors.InvalidArgument);
            done();
        });
    });
});
