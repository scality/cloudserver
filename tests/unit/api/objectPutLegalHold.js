const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutLegalHold = require('../../../lib/api/objectPutLegalHold');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();

const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('body', 'utf8');

const putBucketRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const putObjectRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
}, postBody);

const objectLegalHoldXml = status => `<LegalHold 
    xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Status>${status}</Status>
    </LegalHold>`;

const putLegalHoldReq = status => ({
    bucketName,
    objectKey: objectName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: objectLegalHoldXml(status),
});

describe('putObjectLegalHold API', () => {
    before(cleanup);

    describe('without Object Lock enabled on bucket', () => {
        beforeEach(done => {
            bucketPut(authInfo, putBucketRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });
        afterEach(cleanup);

        it('should return InvalidRequest error', done => {
            objectPutLegalHold(authInfo, putLegalHoldReq('ON'), log, err => {
                assert.strictEqual(err.InvalidRequest, true);
                done();
            });
        });
    });

    describe('with Object Lock enabled on bucket', () => {
        const bucketObjLockRequest = Object.assign({}, putBucketRequest,
            { headers: { 'x-amz-bucket-object-lock-enabled': true } });

        beforeEach(done => {
            bucketPut(authInfo, bucketObjLockRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });
        afterEach(cleanup);

        it('should update object\'s metadata with legal hold status', done => {
            objectPutLegalHold(authInfo, putLegalHoldReq('ON'), log, err => {
                assert.ifError(err);
                return metadata.getObjectMD(bucketName, objectName, {}, log,
                (err, objMD) => {
                    assert.ifError(err);
                    assert.strictEqual(objMD.legalHold, true);
                    return done();
                });
            });
        });

        it('should update object\'s metadata with legal hold status', done => {
            objectPutLegalHold(authInfo, putLegalHoldReq('OFF'), log, err => {
                assert.ifError(err);
                return metadata.getObjectMD(bucketName, objectName, {}, log,
                (err, objMD) => {
                    assert.ifError(err);
                    assert.strictEqual(objMD.legalHold, false);
                    return done();
                });
            });
        });
    });
});
