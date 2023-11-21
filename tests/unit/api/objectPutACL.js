const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const constants = require('../../../constants');
const {
    cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    AccessControlPolicy,
} = require('../helpers');
const metadata = require('../metadataswitch');
const objectPut = require('../../../lib/api/objectPut');
const objectPutACL = require('../../../lib/api/objectPutACL');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const ownerID = authInfo.getCanonicalID();
const anotherID = '79a59df900b949e55d96a1e698fba'
    + 'cedfd6e09d98eacf8f8d5218e7cd47ef2bf';
const defaultAcpParams = {
    ownerID,
    ownerDisplayName: 'OwnerDisplayName',
};
const namespace = 'default';
const bucketName = 'bucketname';
const postBody = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';
const objectName = 'objectName';
const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
let testPutObjectRequest;

describe('putObjectACL API', () => {
    beforeEach(() => {
        cleanup();
        testPutObjectRequest = new DummyRequest({
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        }, postBody);
    });

    it('should return an error if invalid canned ACL provided', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'invalid-option' },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.InvalidArgument, true);
                        done();
                    });
                });
        });
    });

    it('should set a canned public-read-write ACL', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'public-read-write' },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(md.acl.Canned,
                                    'public-read-write');
                                assert.strictEqual(md.originOp, 's3:ObjectAcl:Put');
                                done();
                            });
                    });
                });
        });
    });

    it('should set a canned public-read ACL followed by'
        + ' a canned authenticated-read ACL', done => {
        const testObjACLRequest1 = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'public-read' },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        const testObjACLRequest2 = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'authenticated-read' },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest1, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(md.acl.Canned,
                                    'public-read');
                                objectPutACL(authInfo, testObjACLRequest2, log,
                                    err => {
                                        assert.strictEqual(err, null);
                                        metadata.getObjectMD(bucketName,
                                            objectName, {}, log, (err, md) => {
                                                assert.strictEqual(md
                                                    .acl.Canned,
                                                'authenticated-read');
                                                assert.strictEqual(md.originOp, 's3:ObjectAcl:Put');
                                                done();
                                            });
                                    });
                            });
                    });
                });
        });
    });

    it('should set ACLs provided in request headers', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"'
                    + ',emailaddress="sampleaccount2@sampling.com"',
                'x-amz-grant-read': `uri=${constants.logId}`,
                'x-amz-grant-read-acp': `id=${ownerID}`,
                'x-amz-grant-write-acp': `id=${anotherID}`,
            },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };
        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(err, null);
                                const acls = md.acl;
                                assert.strictEqual(acls.READ[0],
                                    constants.logId);
                                assert(acls.FULL_CONTROL[0]
                                    .indexOf(ownerID) > -1);
                                assert(acls.FULL_CONTROL[1]
                                    .indexOf(anotherID) > -1);
                                assert(acls.READ_ACP[0]
                                    .indexOf(ownerID) > -1);
                                assert(acls.WRITE_ACP[0]
                                    .indexOf(anotherID) > -1);
                                assert.strictEqual(md.originOp, 's3:ObjectAcl:Put');
                                done();
                            });
                    });
                });
        });
    });

    it('should return an error if invalid email '
        + 'provided in ACL header request', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'x-amz-grant-full-control':
                    'emailaddress="sampleaccount1@sampling.com"'
                    + ',emailaddress="nonexistentemail@sampling.com"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.UnresolvableGrantByEmailAddress, true);
                        done();
                    });
                });
        });
    });

    it('should set ACLs provided in request body', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('CanonicalUser', ownerID, 'FULL_CONTROL',
            'OwnerDisplayName');
        acp.addGrantee('Group', constants.publicId, 'READ');
        acp.addGrantee('AmazonCustomerByEmail', 'sampleaccount1@sampling.com',
            'WRITE_ACP');
        acp.addGrantee('CanonicalUser', anotherID, 'READ_ACP');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(acp.getXml(), 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined,
                log, (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(md
                                    .acl.FULL_CONTROL[0], ownerID);
                                assert.strictEqual(md
                                    .acl.READ[0], constants.publicId);
                                assert.strictEqual(md
                                    .acl.WRITE_ACP[0], ownerID);
                                assert.strictEqual(md
                                    .acl.READ_ACP[0], anotherID);
                                assert.strictEqual(md.originOp, 's3:ObjectAcl:Put');
                                done();
                            });
                    });
                });
        });
    });

    it('should return an error if wrong owner ID '
    + 'provided in ACLs set out in request body', done => {
        const acp = new AccessControlPolicy({ ownerID: anotherID });
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(acp.getXml(), 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                () => {
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.AccessDenied, true);
                        done();
                    });
                });
        });
    });

    it('should ignore if WRITE ACL permission is '
        + 'provided in request body', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('CanonicalUser', ownerID, 'FULL_CONTROL',
            'OwnerDisplayName');
        acp.addGrantee('Group', constants.publicId, 'WRITE');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(acp.getXml(), 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(md.acl.Canned, '');
                                assert.strictEqual(md.acl.FULL_CONTROL[0],
                                    ownerID);
                                assert.strictEqual(md.acl.WRITE, undefined);
                                assert.strictEqual(md.acl.READ[0], undefined);
                                assert.strictEqual(md.acl.WRITE_ACP[0],
                                    undefined);
                                assert.strictEqual(md.acl.READ_ACP[0],
                                    undefined);
                                done();
                            });
                    });
                });
        });
    });

    it('should return an error if invalid email '
    + 'address provided in ACLs set out in request body', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('AmazonCustomerByEmail', 'xyz@amazon.com', 'WRITE_ACP');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(acp.getXml(), 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };


        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.UnresolvableGrantByEmailAddress, true);
                        done();
                    });
                });
        });
    });

    it('should return an error if xml provided does not match s3 '
    + 'scheme for setting ACLs', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('AmazonCustomerByEmail', 'xyz@amazon.com', 'WRITE_ACP');
        const originalXml = acp.getXml();
        const modifiedXml = originalXml.replace(/Grant/g, 'PowerGrant');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(modifiedXml, 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.MalformedACLError, true);
                        done();
                    });
                });
        });
    });

    it('should return an error if malformed xml provided', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('AmazonCustomerByEmail', 'xyz@amazon.com', '');
        const originalXml = acp.getXml();
        const modifiedXml = originalXml.replace(/<Grant/, '</Grant');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(modifiedXml, 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };


        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.MalformedXML, true);
                        done();
                    });
                });
        });
    });

    it('should return an error if invalid group '
    + 'uri provided in ACLs set out in request body', done => {
        const acp = new AccessControlPolicy(defaultAcpParams);
        acp.addGrantee('Group', 'http://acs.amazonaws.com/groups/'
        + 'global/NOTAVALIDGROUP', 'WRITE_ACP');
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}?acl`,
            post: [Buffer.from(acp.getXml(), 'utf8')],
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.InvalidArgument, true);
                        done();
                    });
                });
        });
    });

    it('should return an error if invalid group uri '
        + 'provided in ACL header request', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {
                'host': 's3.amazonaws.com',
                'x-amz-grant-full-control':
                    'uri="http://acs.amazonaws.com/groups/'
                    + 'global/NOTAVALIDGROUP"',
            },
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
            actionImplicitDenies: false,
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err.is.InvalidArgument, true);
                        done();
                    });
                });
        });
    });

    [
        { headers: { 'x-amz-acl': 'public-read-write' }, type: 'ACL' },
        { headers: { 'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"' }, type: 'FULL_CONTROL' },
        { headers: { 'x-amz-grant-read': `uri=${constants.logId}` }, type: 'READ' },
        { headers: { 'x-amz-grant-read-acp': `id=${ownerID}` }, type: 'READ_ACP' },
        { headers: { 'x-amz-grant-write-acp': `id=${anotherID}` }, type: 'WRITE_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-read-acp': `id=${ownerID}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'ALL' },
        { headers: {
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-read-acp': `id=${ownerID}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'READ/READ_ACP/WRITE_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read-acp': `id=${ownerID}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'FULL/READ_ACP/WRITE_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'FULL/READ/WRITE_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-read-acp': `id=${ownerID}`,
        }, type: 'FULL/READ/READ_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read': `uri=${constants.logId}`,
        }, type: 'FULL/READ' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-read-acp': `id=${ownerID}`,
        }, type: 'FULL/READ_ACP' },
        { headers: {
            'x-amz-grant-full-control': 'emailaddress="sampleaccount1@sampling.com"',
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'FULL/WRITE_ACP' },
        { headers: {
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-read-acp': `id=${ownerID}`,
        }, type: 'READ/READ_ACP' },
        { headers: {
            'x-amz-grant-read': `uri=${constants.logId}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'READ/WRITE_ACP' },
        { headers: {
            'x-amz-grant-read-acp': `id=${ownerID}`,
            'x-amz-grant-write-acp': `id=${anotherID}`,
        }, type: 'READ_ACP/WRITE_ACP' },
    ].forEach(params => {
        const { headers, type } = params;
        it(`should set originOp to s3:ObjectAcl:Put when ACL is changed (${type})`, done => {
            const testObjACLRequest = {
                bucketName,
                namespace,
                objectKey: objectName,
                headers,
                url: `/${bucketName}/${objectName}?acl`,
                query: { acl: '' },
            };
            bucketPut(authInfo, testPutBucketRequest, log, () => {
                objectPut(authInfo, testPutObjectRequest, undefined, log,
                    (err, resHeaders) => {
                        assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                        objectPutACL(authInfo, testObjACLRequest, log, err => {
                            assert.strictEqual(err, null);
                            metadata.getObjectMD(bucketName, objectName, {},
                            log, (err, md) => {
                                assert.strictEqual(md.originOp,
                                's3:ObjectAcl:Put');
                                done();
                            });
                        });
                    });
            });
        });
    });

    it('should keep original originOp when ACL request do not produce change', done => {
        const testObjACLRequest = {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: { 'x-amz-acl': 'private' }, // keeping the default value
            url: `/${bucketName}/${objectName}?acl`,
            query: { acl: '' },
        };

        bucketPut(authInfo, testPutBucketRequest, log, () => {
            objectPut(authInfo, testPutObjectRequest, undefined, log,
                (err, resHeaders) => {
                    assert.strictEqual(resHeaders.ETag, `"${correctMD5}"`);
                    objectPutACL(authInfo, testObjACLRequest, log, err => {
                        assert.strictEqual(err, null);
                        metadata.getObjectMD(bucketName, objectName, {},
                        log, (err, md) => {
                            assert.strictEqual(md.originOp,
                            's3:ObjectCreated:Put');
                            done();
                        });
                    });
                });
        });
    });
});
