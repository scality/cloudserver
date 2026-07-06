const assert = require('assert');
const sinon = require('sinon');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketUpdateQuota = require('../../../lib/api/bucketUpdateQuota');
const objectPut = require('../../../lib/api/objectPut');
const { config } = require('../../../lib/Config');
const DummyRequest = require('../DummyRequest');
const metadata = require('../metadataswitch');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'quotabucketname';

const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

const updateQuotaRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
        'content-type': 'application/json',
    },
    post: '{"quota": 1000}',
    actionImplicitDenies: false,
};

function putObject(objectName, cb) {
    const request = new DummyRequest(
        {
            bucketName,
            namespace,
            objectKey: objectName,
            headers: {},
            url: `/${bucketName}/${objectName}`,
        },
        Buffer.from('body content', 'utf8'),
    );
    return objectPut(authInfo, request, undefined, log, cb);
}

describe('bucketUpdateQuota API quota metric seeding', () => {
    beforeEach(() => {
        sinon.stub(config, 'isQuotaEnabled').returns(true);
    });

    afterEach(() => {
        sinon.restore();
        cleanup();
    });

    it('should seed a zero-value metric when a quota is enabled on an empty bucket', done => {
        const initStub = sinon
            .stub(metadata, 'initializeBucketCapacity')
            .callsFake((name, creationDate, l, cb) => process.nextTick(() => cb(null)));
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initStub.resetHistory();
            return bucketUpdateQuota(authInfo, updateQuotaRequest, log, err => {
                assert.ifError(err);
                assert(initStub.calledOnce, 'expected seeding for an empty bucket');
                assert.strictEqual(initStub.firstCall.args[0], bucketName);
                done();
            });
        });
    });

    it('should not seed a metric when the bucket is not empty', done => {
        const initStub = sinon
            .stub(metadata, 'initializeBucketCapacity')
            .callsFake((name, creationDate, l, cb) => process.nextTick(() => cb(null)));
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            return putObject('someobject', err => {
                assert.ifError(err);
                initStub.resetHistory();
                return bucketUpdateQuota(authInfo, updateQuotaRequest, log, err => {
                    assert.ifError(err);
                    assert(initStub.notCalled, 'expected no seeding for a non-empty bucket');
                    done();
                });
            });
        });
    });

    it('should still update the quota if seeding the metric fails', done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            sinon
                .stub(metadata, 'initializeBucketCapacity')
                .callsFake((name, creationDate, l, cb) => process.nextTick(() => cb(errors.InternalError)));
            return bucketUpdateQuota(authInfo, updateQuotaRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });

    it('should not seed a metric when quotas are disabled', done => {
        config.isQuotaEnabled.restore();
        sinon.stub(config, 'isQuotaEnabled').returns(false);
        const initStub = sinon
            .stub(metadata, 'initializeBucketCapacity')
            .callsFake((name, creationDate, l, cb) => process.nextTick(() => cb(null)));
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            initStub.resetHistory();
            return bucketUpdateQuota(authInfo, updateQuotaRequest, log, err => {
                assert.ifError(err);
                assert(initStub.notCalled, 'expected no seeding when quotas are disabled');
                done();
            });
        });
    });
});
