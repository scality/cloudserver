import { errors } from 'arsenal';
import assert from 'assert';
import crypto from 'crypto';

import BucketInfo from '../../../lib/metadata/BucketInfo';
import bucketGet from '../../../lib/api/bucketGet';
import bucketGetACL from '../../../lib/api/bucketGetACL';
import bucketGetWebsite from '../../../lib/api/bucketGetWebsite';
import bucketHead from '../../../lib/api/bucketHead';
import bucketPut from '../../../lib/api/bucketPut';
import bucketPutACL from '../../../lib/api/bucketPutACL';
import bucketPutWebsite from '../../../lib/api/bucketPutWebsite';
import bucketDelete from '../../../lib/api/bucketDelete';
import completeMultipartUpload from
    '../../../lib/api/completeMultipartUpload';
import constants from '../../../constants';
import DummyRequest from '../DummyRequest';
import initiateMultipartUpload from
    '../../../lib/api/initiateMultipartUpload';
import { cleanup, createAlteredRequest } from '../helpers';
import listMultipartUploads from '../../../lib/api/listMultipartUploads';
import listParts from '../../../lib/api/listParts';
import metadata from '../metadataswitch';
import multipartDelete from '../../../lib/api/multipartDelete';
import objectDelete from '../../../lib/api/objectDelete';
import objectGet from '../../../lib/api/objectGet';
import objectGetACL from '../../../lib/api/objectGetACL';
import objectHead from '../../../lib/api/objectHead';
import objectPut from '../../../lib/api/objectPut';
import objectPutACL from '../../../lib/api/objectPutACL';
import objectPutPart from '../../../lib/api/objectPutPart';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import { parseString } from 'xml2js';
import serviceGet from '../../../lib/api/serviceGet';

const log = new DummyRequestLogger();
const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const otherAccountAuthInfo = makeAuthInfo('accessKey2');
const namespace = 'default';
const usersBucketName = constants.usersBucket;
const bucketName = 'bucketname';
const locationConstraint = 'us-west-1';
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
                    assert.deepStrictEqual(err, errors.NoSuchBucket);
                    return checkBucketListing(authInfo, bucketName, 0, done);
                });
            });
        });
    });
}


describe('deleted flag bucket handling', () => {
    beforeEach(done => {
        cleanup();
        const bucketMD = new BucketInfo(bucketName, canonicalID,
            authInfo.getAccountDisplayName(), creationDate);
        bucketMD.addDeletedFlag();
        bucketMD.setSpecificAcl(otherAccountAuthInfo.getCanonicalID(),
            'FULL_CONTROL');
        metadata.createBucket(bucketName, bucketMD, log, () => {
            metadata.createBucket(usersBucketName, usersBucket, log, () => {
                done();
            });
        });
    });

    it('putBucket request should recreate bucket with deleted flag if ' +
        'request is from same account that originally put', done => {
        bucketPut(authInfo, baseTestRequest, locationConstraint, log, err => {
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
        bucketPut(otherAccountAuthInfo, baseTestRequest, locationConstraint,
            log, err => {
                assert.deepStrictEqual(err, errors.BucketAlreadyExists);
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
        bucketPut(authInfo, alteredRequest, locationConstraint, log, err => {
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
                assert.deepStrictEqual(err, errors.NoSuchBucket);
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
                assert.deepStrictEqual(err, errors.AccessDenied);
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
            metadata.deleteObjectMD(bucketName, objName, log, () => {
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
                    metadata.getObjectMD(bucketName, objName, log,
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
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    describe('initiateMultipartUpload on a bucket with deleted flag', () => {
        const objName = 'objectName';
        after(done => {
            metadata.deleteObjectMD(`${constants.mpuBucketPrefix}` +
                `${bucketName}`, objName, log, () => {
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
                assert.deepStrictEqual(err, errors.NoSuchBucket);
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
                assert.deepStrictEqual(err, errors.AccessDenied);
                done();
            });
    });

    it('bucketGet request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketGet(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                confirmDeleted(done);
            });
    });

    it('bucketGetACL request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketGetACL(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                confirmDeleted(done);
            });
    });

    it('bucketGetWebsite request on bucket with delete flag should return ' +
    'NoSuchBucket error and complete deletion', done => {
        bucketGetWebsite(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
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
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            confirmDeleted(done);
        });
    });

    it('bucketHead request on bucket with delete flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        bucketHead(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
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
                    assert.deepStrictEqual(err, errors.NoSuchUpload);
                    return done();
                });
        }
        return apiAction(authInfo, mpuRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
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

    it('multipartDelete request on bucket with deleted flag should ' +
        'return NoSuchUpload error', done => {
        checkForNoSuchUploadError(multipartDelete, null, done);
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
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    it('objectGet request on bucket with deleted flag should' +
        'return NoSuchBucket error and finish deletion',
        done => {
            objectGet(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                confirmDeleted(done);
            });
        });

    it('objectGetACL request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectGetACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            confirmDeleted(done);
        });
    });

    it('objectHead request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectHead(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            confirmDeleted(done);
        });
    });

    it('objectPutACL request on bucket with deleted flag should return ' +
        'NoSuchBucket error and complete deletion', done => {
        objectPutACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            confirmDeleted(done);
        });
    });

    it('objectDelete request on bucket with deleted flag should return ' +
        'NoSuchBucket error', done => {
        objectDelete(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            confirmDeleted(done);
        });
    });
});
