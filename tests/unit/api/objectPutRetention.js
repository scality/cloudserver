const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutRetention = require('../../../lib/api/objectPutRetention');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');

const date = new Date();
date.setDate(date.getDate() + 1);

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

const objectRetentionXml = '<ObjectRetention ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    '<Mode>GOVERNANCE</Mode>' +
    `<RetainUntilDate>${date.toISOString()}</RetainUntilDate>` +
    '</ObjectRetention>';

const putObjRetRequest = {
    bucketName,
    objectKey: objectName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    post: objectRetentionXml,
};

const expectedRetInfo = {
    retention: {
        mode: 'GOVERNANCE',
        retainUntilDate: date.toISOString(),
    },
};

describe('putObjectRetention API', () => {
    before(() => cleanup());

    describe('without Object Lock enabled on bucket', () => {
        beforeEach(done => {
            bucketPut(authInfo, bucketPutRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });
        afterEach(() => cleanup());

        it('should return InvalidRequest error', done => {
            objectPutRetention(authInfo, putObjRetRequest, log, err => {
                assert.strictEqual(err.InvalidRequest, true);
                done();
            });
        });
    });

    describe('with Object Lock enabled on bucket', () => {
        const bucketObjLockRequest = Object.assign({}, bucketPutRequest,
            { headers: { 'x-amz-bucket-object-lock-enabled': true } });

        beforeEach(done => {
            bucketPut(authInfo, bucketObjLockRequest, log, err => {
                assert.ifError(err);
                objectPut(authInfo, putObjectRequest, undefined, log, done);
            });
        });
        afterEach(() => cleanup());

        it('should update an object\'s metadata with retention info', done => {
            objectPutRetention(authInfo, putObjRetRequest, log, err => {
                assert.ifError(err);
                return metadata.getObjectMD(bucketName, objectName, {}, log,
                (err, objMD) => {
                    assert.ifError(err);
                    assert.deepStrictEqual(objMD.retentionInfo,
                        expectedRetInfo);
                    return done();
                });
            });
        });
    });
});
