const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutLegalHold = require('../../../lib/api/objectPutLegalHold');
const objectGetLegalHold = require('../../../lib/api/objectGetLegalHold');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');

const bucketPutRequest = {
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

const objectLegalHoldXml = status =>
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    `<LegalHold><Status>${status}</Status></LegalHold>`;

const putObjectLegalHoldRequest = status => ({
    bucketName,
    objectKey: objectName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: objectLegalHoldXml(status),
});

const getObjectLegalHoldRequest = {
    bucketName,
    objectKey: objectName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
};

describe('getObjectLegalHold API', () => {
    before(cleanup);

    describe('without Object Lock enabled on bucket', () => {
        beforeEach(done => {
            bucketPut(authInfo, bucketPutRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });

        afterEach(cleanup);

        it('should return InvalidRequest error', done => {
            objectGetLegalHold(authInfo, getObjectLegalHoldRequest, log,
                err => {
                    assert.strictEqual(err.InvalidRequest, true);
                    done();
                });
        });
    });

    describe('with Object Lock enabled on bucket', () => {
        const bucketObjectLockRequest = Object.assign({}, bucketPutRequest,
            { headers: { 'x-amz-bucket-object-lock-enabled': 'true' } });

        beforeEach(done => {
            bucketPut(authInfo, bucketObjectLockRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });

        afterEach(cleanup);

        it('should return NoSuchObjectLockConfiguration if no legal hold set',
            done => {
                objectGetLegalHold(authInfo, getObjectLegalHoldRequest, log,
                    err => {
                        const error = err.NoSuchObjectLockConfiguration;
                        assert.strictEqual(error, true);
                        done();
                    });
            });

        it('should get an object\'s legal hold status when OFF', done => {
            const status = 'OFF';
            const request = putObjectLegalHoldRequest(status);
            objectPutLegalHold(authInfo, request, log, err => {
                assert.ifError(err);
                objectGetLegalHold(authInfo, getObjectLegalHoldRequest, log,
                    (err, xml) => {
                        const expectedXml = objectLegalHoldXml(status);
                        assert.ifError(err);
                        assert.strictEqual(xml, expectedXml);
                        done();
                    });
            });
        });

        it('should get an object\'s legal hold status when ON', done => {
            const status = 'ON';
            const request = putObjectLegalHoldRequest(status);
            objectPutLegalHold(authInfo, request, log, err => {
                assert.ifError(err);
                objectGetLegalHold(authInfo, getObjectLegalHoldRequest, log,
                    (err, xml) => {
                        const expectedXml = objectLegalHoldXml(status);
                        assert.ifError(err);
                        assert.strictEqual(xml, expectedXml);
                        done();
                    });
            });
        });
    });
});
