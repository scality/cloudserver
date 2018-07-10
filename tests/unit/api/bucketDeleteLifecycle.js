const assert = require('assert');
const async = require('async');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutLifecycle = require('../../../lib/api/bucketDeleteLifecycle');
const bucketDeleteLifecycle = require('../../../lib/api/bucketDeleteLifecycle');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

function _makeRequest(includeXml) {
    const request = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    };
    if (includeXml) {
        request.post = '<LifecycleConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Rule><ID></ID><Filter></Filter>' +
        '<Status>Enabled</Status>' +
        '<Expiration><Days>1</Days></Expiration>' +
        '</Rule></LifecycleConfiguration>';
    }
    return request;
}

describe('deleteBucketLifecycle API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, _makeRequest(), log, done));
    afterEach(() => cleanup());

    it('should not return an error even if no lifecycle put', done => {
        bucketDeleteLifecycle(authInfo, _makeRequest(), log, err => {
            assert.equal(err, null, `Err deleting lifecycle: ${err}`);
            done();
        });
    });
    it('should delete bucket lifecycle', done => {
        async.series([
            next => bucketPutLifecycle(authInfo, _makeRequest(true), log, next),
            next => bucketDeleteLifecycle(authInfo, _makeRequest(), log, next),
            next => metadata.getBucket(bucketName, log, next),
        ], (err, results) => {
            assert.equal(err, null, `Expected success, got error: ${err}`);
            const bucket = results[2];
            const lifecycleConfig = bucket.getLifecycleConfiguration();
            assert.equal(lifecycleConfig, null);
            done();
        });
    });
});
