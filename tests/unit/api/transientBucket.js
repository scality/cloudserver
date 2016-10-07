import assert from 'assert';
import crypto from 'crypto';
import { errors } from 'arsenal';

import BucketInfo from '../../../lib/metadata/BucketInfo';
import bucketGet from '../../../lib/api/bucketGet';
import bucketGetACL from '../../../lib/api/bucketGetACL';
import bucketHead from '../../../lib/api/bucketHead';
import bucketPut from '../../../lib/api/bucketPut';
import bucketPutACL from '../../../lib/api/bucketPutACL';
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
const baseTestRequest = {
    bucketName,
    namespace,
    url: '/',
    post: '',
    headers: { host: `${bucketName}.s3.amazonaws.com` },
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
const locationConstraint = 'us-west-1';


describe('transient bucket handling', () => {
    beforeEach(done => {
        cleanup();
        const bucketMD = new BucketInfo(bucketName, canonicalID,
            authInfo.getAccountDisplayName(), creationDate);
        bucketMD.addTransientFlag();
        bucketMD.setSpecificAcl(otherAccountAuthInfo.getCanonicalID(),
            'WRITE_ACP');
        metadata.createBucket(bucketName, bucketMD, log, () => {
            metadata.createBucket(usersBucketName, usersBucket, log, () => {
                done();
            });
        });
    });

    it('putBucket request should complete creation of transient bucket if ' +
        'request is from same account that originally put', done => {
        bucketPut(authInfo, baseTestRequest, locationConstraint, log, err => {
            assert.ifError(err);
            serviceGet(authInfo, serviceGetRequest, log, (err, data) => {
                parseString(data, (err, result) => {
                    assert.strictEqual(result.ListAllMyBucketsResult
                        .Buckets[0].Bucket.length, 1);
                    assert.strictEqual(result.ListAllMyBucketsResult
                        .Buckets[0].Bucket[0].Name[0], bucketName);
                    done();
                });
            });
        });
    });

    it('putBucket request should return error if ' +
        'transient bucket created by different account', done => {
        bucketPut(otherAccountAuthInfo, baseTestRequest, locationConstraint,
            log, err => {
                assert.deepStrictEqual(err, errors.BucketAlreadyExists);
                serviceGet(otherAccountAuthInfo, serviceGetRequest,
                    log, (err, data) => {
                        parseString(data, (err, result) => {
                            assert.strictEqual(result.ListAllMyBucketsResult
                            .Buckets[0], '');
                            done();
                        });
                    });
            });
    });

    it('ACLs from clean up putBucket request should overwrite ACLs from ' +
        'original failed request that resulted in transient state', done => {
        const alteredRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPut(authInfo, alteredRequest, locationConstraint, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._acl.Canned, 'public-read');
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                done();
            });
        });
    });

    it('putBucketACL request should complete creation of transient bucket if ' +
        'request is from same account that originally put', done => {
        const putACLRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        putACLRequest.url = '/?acl';
        putACLRequest.query = { acl: '' };
        bucketPutACL(authInfo, putACLRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._acl.Canned, 'public-read');
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                done();
            });
        });
    });

    it('putBucketACL request should complete creation of transient bucket if ' +
        'request is from another authorized account', done => {
        const putACLRequest = createAlteredRequest({
            'x-amz-acl': 'public-read' }, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketPutACL(otherAccountAuthInfo, putACLRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, (err, data) => {
                assert.strictEqual(data._transient, false);
                assert.strictEqual(data._acl.Canned, 'public-read');
                assert.strictEqual(data._owner, authInfo.getCanonicalID());
                done();
            });
        });
    });

    describe('objectPut on a transient bucket', () => {
        const objName = 'objectName';
        after(done => {
            metadata.deleteObjectMD(bucketName, objName, log, () => {
                done();
            });
        });

        it('objectPut request should complete creation of transient bucket',
        done => {
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
                    assert.strictEqual(data._owner, authInfo.getCanonicalID());
                    metadata.getObjectMD(bucketName, objName, log,
                        (err, obj) => {
                            assert.ifError(err);
                            assert.strictEqual(obj['content-md5'], etag);
                            done();
                        });
                });
            });
        });
    });

    describe('initiateMultipartUpload on a transient bucket', () => {
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

        it('initiateMultipartUpload request should complete ' +
            'creation of transient bucket', done => {
            const initiateRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
            initiateRequest.objectKey = objName;
            initiateRequest.url = `/${objName}?uploads`;
            initiateMultipartUpload(authInfo, initiateRequest, log, err => {
                assert.ifError(err);
                metadata.getBucket(bucketName, log, (err, data) => {
                    assert.strictEqual(data._transient, false);
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

    it('deleteBucket request should delete transient bucket if ' +
        'request is from owner', done => {
        bucketDelete(authInfo, baseTestRequest, log, err => {
            assert.ifError(err);
            metadata.getBucket(bucketName, log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });
    });

    it('deleteBucket request should return error if ' +
        'request is not from owner', done => {
        bucketDelete(otherAccountAuthInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.AccessDenied);
                done();
            });
    });

    it('bucketGet request on transient bucket should return NoSuchBucket' +
        'error', done => {
        const bucketGetRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketGetRequest.url = `/${bucketName}`;
        bucketGetRequest.query = {};
        bucketGet(authInfo, bucketGetRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    it('bucketGetACL request on transient bucket should return NoSuchBucket' +
        'error', done => {
        const bucketGetACLRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        bucketGetACLRequest.url = '/?acl';
        bucketGetACLRequest.query = { acl: '' };
        bucketGetACL(authInfo, bucketGetACLRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    it('bucketHead request on transient bucket should return NoSuchBucket' +
        'error', done => {
        bucketHead(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
    });

    it('completeMultipartUpload request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const completeMpuRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        completeMpuRequest.objectKey = 'objectName';
        completeMpuRequest.query = { uploadId };
        completeMultipartUpload(authInfo, completeMpuRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    it('listParts request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const listRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        listRequest.objectKey = 'objectName';
        listRequest.query = { uploadId };
        listParts(authInfo, listRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    it('multipartDelete request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const deleteRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        deleteRequest.objectKey = 'objectName';
        deleteRequest.query = { uploadId };
        multipartDelete(authInfo, deleteRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    it('objectPutPart request on transient bucket should ' +
        'return NoSuchUpload error', done => {
        const putPartRequest = createAlteredRequest({}, 'headers',
            baseTestRequest, baseTestRequest.headers);
        const uploadId = '5555';
        putPartRequest.objectKey = 'objectName';
        putPartRequest.query = {
            uploadId,
            partNumber: '1' };
        objectPutPart(authInfo, putPartRequest, undefined,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchUpload);
                done();
            });
    });

    it('list multipartUploads request on transient bucket should ' +
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

    it('objectGet request on transient bucket should' +
        'return NoSuchBucket error',
        done => {
            objectGet(authInfo, baseTestRequest,
            log, err => {
                assert.deepStrictEqual(err, errors.NoSuchBucket);
                done();
            });
        });

    it('objectGetACL request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectGetACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('objectHead request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectHead(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('objectPutACL request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectPutACL(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });

    it('objectDelete request on transient bucket should return ' +
        'NoSuchBucket error', done => {
        objectDelete(authInfo, baseTestRequest,
        log, err => {
            assert.deepStrictEqual(err, errors.NoSuchBucket);
            done();
        });
    });
});
