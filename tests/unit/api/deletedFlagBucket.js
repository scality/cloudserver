const crypto = require('crypto');
const assert = require('assert');

const BucketInfo = require('arsenal').models.BucketInfo;
const bucketGet = require('../../../lib/api/bucketGet');
const bucketGetACL = require('../../../lib/api/bucketGetACL');
const bucketGetCors = require('../../../lib/api/bucketGetCors');
const bucketGetWebsite = require('../../../lib/api/bucketGetWebsite');
const bucketHead = require('../../../lib/api/bucketHead');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutACL = require('../../../lib/api/bucketPutACL');
const bucketPutCors = require('../../../lib/api/bucketPutCors');
const bucketPutWebsite = require('../../../lib/api/bucketPutWebsite');
const bucketDelete = require('../../../lib/api/bucketDelete');
const bucketDeleteCors = require('../../../lib/api/bucketDeleteCors');
const bucketDeleteWebsite = require('../../../lib/api/bucketDeleteWebsite');
const completeMultipartUpload
    = require('../../../lib/api/completeMultipartUpload');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const DummyRequest = require('../DummyRequest');
const initiateMultipartUpload
    = require('../../../lib/api/initiateMultipartUpload');
const { cleanup,
    createAlteredRequest,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const listMultipartUploads = require('../../../lib/api/listMultipartUploads');
const listParts = require('../../../lib/api/listParts');
const metadata = require('../metadataswitch');
const multipartDelete = require('../../../lib/api/multipartDelete');
const objectDelete = require('../../../lib/api/objectDelete');
const objectGet = require('../../../lib/api/objectGet');
const objectGetACL = require('../../../lib/api/objectGetACL');
const objectHead = require('../../../lib/api/objectHead');
const objectPut = require('../../../lib/api/objectPut');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutPart = require('../../../lib/api/objectPutPart');
const { parseString } = require('xml2js');

const serviceGet = require('../../../lib/api/serviceGet');

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const otherAccountAuthInfo = makeAuthInfo('accessKey2');
const namespace = 'default';
const usersBucketName = constants.usersBucket;
const bucketName = 'bucketname';
const locationConstraint = 'us-east-1';

const baseTestRequest = {
    bucketName,
    namespace,
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    query: {},
};
const serviceGetRequest = {
    parsedHost: 's3.amazonaws.com',
    headers: { host: 's3.amazonaws.com' },
    url: '/',
};

const userBucketOwner = 'admin';
const creationDate = new Date().toJSON();
const usersBucket = new BucketInfo(usersBucketName,
    userBucketOwner, userBucketOwner, creationDate);


function checkBucketListing(authInfo, bucketName, expectedListingLength, done) {
    return serviceGet(authInfo, serviceGetRequest, log, (err, data) => {
        parseString(data, (err, result) => {
            if (expectedListingLength > 0) {
                assert.strictEqual(result.ListAllMyBucketsResult
                    .Buckets[0].Bucket.length, expectedListingLength);
                assert.strictEqual(result.ListAllMyBucketsResult
                    .Buckets[0].Bucket[0].Name[0], bucketName);
            } else {
                assert.strictEqual(result.ListAllMyBucketsResult
                    .Buckets[0].length, 0);
            }
            done();
        });
    });
}

function confirmDeleted(done) {
    // Note that we need the process.nextTick's because of the
    // invisiblyDelete calls
    process.nextTick(() => {
        process.nextTick(() => {
            process.nextTick(() => {
                metadata.getBucket(bucketName, log, err => {
                    assert.strictEqual(err.is.NoSuchBucket, true);
                    return checkBucketListing(authInfo, bucketName, 0, done);
                });
            });
        });
    });
}

// TODO CLDSRV-431 remove skip
describe.skip('deleted flag bucket handling', () => {
    beforeEach(done => {
        cleanup();
        const bucketMD = new BucketInfo(bucketName, canonicalID,
            authInfo.getAccountDisplayName(), creationDate);
        bucketMD.addDeletedFlag();
        bucketMD.setSpecificAcl(otherAccountAuthInfo.getCanonicalID(),
            'FULL_CONTROL');
        bucketMD.setLocationConstraint(locationConstraint);
        metadata.createBucket(bucketName, bucketMD, log, () => {
            metadata.createBucket(usersBucketName, usersBucket, log, () => {
                done();
            });
        });
    });

    it('putBucket request should recreate bucket with deleted flag if ' +
        'request is from same account that originally put', done => {
        bucketPut(authInfo, baseTestRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._deleted, false);
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                return checkBucketListing(authInfo, bucketName, 1, done);
            });
        });
    });

    it('putBucket request should return error if ' +
        'different account sends put bucket request for bucket with ' +
        'deleted flag', done => {
        bucketPut(otherAccountAuthInfo, baseTestRequest, log, err => {
            assert.strictEqual(err.is.BucketAlreadyExists, true);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._deleted, true);
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                return checkBucketListing(otherAccountAuthInfo,
                    bucketName, 0, done);
            });
        });
    });

    it('ACLs from new putBucket request should overwrite ACLs saved ' +
        'in metadata of bucket with deleted flag', done => {
        const alteredRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPut(authInfo, alteredRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._deleted, false);
                assert.strictEqual(data._acl.Canned, 'public-read');
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                return checkBucketListing(authInfo, bucketName, 1, done);
            });
        });
    });

    it('putBucketACL request should recreate bucket with deleted flag if ' +
        'request is from same account that originally put', done => {
        const putACLRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        putACLRequest.query = { acl: '' };
        bucketPutACL(authInfo, putACLRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._acl.Canned, 'public-read');
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                return checkBucketListing(authInfo, bucketName, 1, done);
            });
        });
    });

    it('putBucketACL request on bucket with deleted flag should return ' +
        'NoSuchBucket error if request is from another authorized account',
        // Do not want different account recreating a bucket that the bucket
        // owner wanted deleted even if the other account is authorized to
        // change the ACLs
        done => {
            const putACLRequest = createAlteredRequest({
                'x-amz-acl': 'public-read' }, 'headers',
                baseTestRequest, baseTestRequest.headers);
            bucketPutACL(otherAccountAuthInfo, putACLRequest, log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                metadata.getBucket(bucketName, log, (err, data) => {
                    assert.strictEqual(data._deleted, true);
                    assert.strictEqual(data._transient, false);
                    assert.strictEqual(data._acl.Canned, 'private');
                    assert.strictEqual(data._owner, authInfo.getCanonicalID());
                    done();
                });
            });
        });

    it('putBucketACL request on bucket with deleted flag should return ' +
        'AccessDenied error if request is from unauthorized account',
        done => {
            const putACLRequest = createAlteredRequest({
                'x-amz-acl': 'public-read' }, 'headers',
                baseTestRequest, baseTestRequest.headers);
            const unauthorizedAccount = makeAuthInfo('keepMeOut');
            bucketPutACL(unauthorizedAccount, putACLRequest, log, err => {
                assert.strictEqual(err.is.AccessDenied, true);
                metadata.getBucket(bucketName, log, (err, data) => {
                    assert.strictEqual(data._deleted, true);
                    assert.strictEqual(data._transient, false);
                    assert.strictEqual(data._acl.Canned, 'private');
                    assert.strictEqual(data._owner, authInfo.getCanonicalID());
                    done();
                });
            });
        });

    describe('objectPut on a bucket with deleted flag', () => {
        const objName = 'objectName';
        afterEach(done => {
            metadata.deleteObjectMD(bucketName, objName, {}, log, () => {
                done();
            });
        });

        it('objectPut request from account that originally created ' +
            'should recreate bucket', done => {
            const setUpRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
            setUpRequest.objectKey = objName;
            const postBody = Buffer.from('I am a body', 'utf8');
            const md5Hash = crypto.createHash('md5');
            const etag = md5Hash.update(postBody).digest('hex');
            const putObjRequest = new DummyRequest(setUpRequest, postBody);
            objectPut(authInfo, putObjRequest, undefined, log, err => {
                assert.ifError(err);
                metadata.getBucket(bucketName, log, (err, data) => {
                    assert.strictEqual(data._transient, false);
                    assert.strictEqual(data._deleted, false);
                    assert.strictEqual(data._owner, authInfo.getCanonicalID());
                    metadata.getObjectMD(bucketName, objName, {}, log,
                        (err, obj) => {
                            assert.ifError(err);
                            assert.strictEqual(obj['content-md5'], etag);
                            return checkBucketListing(authInfo,
                                bucketName, 1, done);
                        });
                });
            });
        });
    });

    it('should return NoSuchBucket error on an objectPut request from ' +
        'different account when there is a deleted flag', done => {
        const setUpRequest = createAlteredRequest({}, 'headers',
        baseTestRequest, baseTestRequest.headers);
        setUpRequest.objectKey = 'objectName';
        const postBody = Buffer.from('I am a body', 'utf8');
        const putObjRequest = new DummyRequest(setUpRequest, postBody);
        objectPut(otherAccountAuthInfo, putObjRequest, undefined, log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            done();
        });
    });

    describe('initiateMultipartUpload on a bucket with deleted flag', () => {
        const objName = 'objectName';
        after(done => {
            metadata.deleteObjectMD(`${constants.mpuBucketPrefix}` +
                `${bucketName}`, objName, {}, log, () => {
                    metadata.deleteBucket(`${constants.mpuBucketPrefix}` +
                        `${bucketName}`, log, () => {
                            done();
                        });
                });
        });

        it('should recreate bucket with deleted flag', done => {
            const initiateRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
            initiateRequest.objectKey = objName;
            initiateRequest.url = `/${objName}?uploads`;
            initiateMultipartUpload(authInfo, initiateRequest, log, err => {
                assert.ifError(err);
                metadata.getBucket(bucketName, log, (err, data) => {
                    assert.strictEqual(data._transient, false);
                    assert.strictEqual(data._deleted, false);
                    assert.strictEqual(data._owner, authInfo.getCanonicalID());
                    metadata.listObject(`${constants.mpuBucketPrefix}` +
                        `${bucketName}`,
                        { prefix: `overview${constants.splitter}${objName}` },
                        log, (err, results) => {
                            assert.ifError(err);
                            assert.strictEqual(results.Contents.length, 1);
                            done();
                        });
                });
            });
        });
    });

    it('should return NoSuchBucket error on an initiateMultipartUpload ' +
        'request from different account when there is a deleted flag', done => {
        const initiateRequest = createAlteredRequest({}, 'headers',
        baseTestRequest, baseTestRequest.headers);
        initiateRequest.objectKey = 'objectName';
        initiateMultipartUpload(otherAccountAuthInfo, initiateRequest, log,
            err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                done();
            });
    });

    it('deleteBucket request should complete deletion ' +
        'of bucket with deleted flag', done => {
        bucketDelete(authInfo, baseTestRequest, log, err => {
            assert.ifError(err);
            confirmDeleted(done);
        });
    });

    it('deleteBucket request should return error if account not ' +
        'authorized', done => {
        bucketDelete(otherAccountAuthInfo, baseTestRequest,
            log, err => {
                assert.strictEqual(err.is.AccessDenied, true);
                done();
            });
    });

    it('bucketDeleteWebsite request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketDeleteWebsite(authInfo, baseTestRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                confirmDeleted(done);
            });
    });

    it('bucketGet request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketGet(authInfo, baseTestRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                confirmDeleted(done);
            });
    });

    it('bucketGetACL request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketGetACL(authInfo, baseTestRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                confirmDeleted(done);
            });
    });

    it('bucketGetCors request on bucket with delete flag should return ' +
    'NoSuchBucket error and complete deletion', done => {
        bucketGetCors(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('bucketPutCors request on bucket with delete flag should return ' +
    'NoSuchBucket error and complete deletion', done => {
        const bucketPutCorsRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutCorsRequest.post = '<CORSConfiguration><CORSRule>' +
        '<AllowedMethod>PUT</AllowedMethod>' +
        '<AllowedOrigin>http://www.example.com</AllowedOrigin>' +
        '</CORSRule></CORSConfiguration>';
        bucketPutCorsRequest.headers['content-md5'] = crypto.createHash('md5')
            .update(bucketPutCorsRequest.post, 'utf8').digest('base64');
        bucketPutCors(authInfo, bucketPutCorsRequest, log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('bucketDeleteCors request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketDeleteCors(authInfo, baseTestRequest, log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('bucketGetWebsite request on bucket with delete flag should return ' +
    'NoSuchBucket error and complete deletion', done => {
        bucketGetWebsite(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('bucketPutWebsite request on bucket with delete flag should return ' +
    'NoSuchBucket error and complete deletion', done => {
        const bucketPutWebsiteRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutWebsiteRequest.post = '<WebsiteConfiguration>' +
        '<IndexDocument><Suffix>index.html</Suffix></IndexDocument>' +
        '</WebsiteConfiguration>';
        bucketPutWebsite(authInfo, bucketPutWebsiteRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('bucketHead request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketHead(authInfo, baseTestRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                confirmDeleted(done);
            });
    });

    function checkForNoSuchUploadError(apiAction, partNumber, done,
        extraArgNeeded) {
        const mpuRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        mpuRequest.objectKey = 'objectName';
        mpuRequest.query = { uploadId, partNumber };
        if (extraArgNeeded) {
            return apiAction(authInfo, mpuRequest, undefined,
                log, err => {
                    assert.strictEqual(err.is.NoSuchUpload, true);
                    return done();
                });
        }
        return apiAction(authInfo, mpuRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchUpload, true);
                return done();
            });
    }

    it('completeMultipartUpload request on bucket with deleted flag should ' +
        'return NoSuchUpload error', done => {
        checkForNoSuchUploadError(completeMultipartUpload, null, done);
    });

    it('listParts request on bucket with deleted flag should ' +
        'return NoSuchUpload error', done => {
        checkForNoSuchUploadError(listParts, null, done);
    });

    describe('multipartDelete request on a bucket with deleted flag', () => {
        it('should return NoSuchUpload error if legacyAWSBehavior is enabled',
        done => {
            config.locationConstraints[locationConstraint].
                legacyAwsBehavior = true;
            checkForNoSuchUploadError(multipartDelete, null, done);
        });

        it('should return no error if legacyAWSBehavior is not enabled',
        done => {
            config.locationConstraints[locationConstraint].
                legacyAwsBehavior = false;
            const mpuRequest = createAlteredRequest({}, 'headers',
                baseTestRequest, baseTestRequest.headers);
            const uploadId = '5555';
            mpuRequest.objectKey = 'objectName';
            mpuRequest.query = { uploadId };
            multipartDelete(authInfo, mpuRequest, log, err => {
                assert.strictEqual(err, null);
                return done();
            });
        });
    });

    it('objectPutPart request on bucket with deleted flag should ' +
        'return NoSuchUpload error', done => {
        checkForNoSuchUploadError(objectPutPart, '1', done, true);
    });

    it('list multipartUploads request on bucket with deleted flag should ' +
        'return NoSuchBucket error', done => {
        const listRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        listRequest.query = {};
        listMultipartUploads(authInfo, listRequest,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                done();
            });
    });

    it('objectGet request on bucket with deleted flag should' +
        'return NoSuchBucket error and finish deletion',
        done => {
            objectGet(authInfo, baseTestRequest, false,
            log, err => {
                assert.strictEqual(err.is.NoSuchBucket, true);
                confirmDeleted(done);
            });
        });

    it('objectGetACL request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectGetACL(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('objectHead request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectHead(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('objectPutACL request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectPutACL(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });

    it('objectDelete request on bucket with deleted flag should return ' +
        'NoSuchBucket error', done => {
        objectDelete(authInfo, baseTestRequest,
        log, err => {
            assert.strictEqual(err.is.NoSuchBucket, true);
            confirmDeleted(done);
        });
    });
});
