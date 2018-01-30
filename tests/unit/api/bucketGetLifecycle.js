const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetLifecycle = require('../../../lib/api/bucketGetLifecycle');
const bucketPutLifecycle = require('../../../lib/api/bucketPutLifecycle');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const { getLifecycleRequest, getLifecycleXml } =
    require('../utils/lifecycleHelpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

describe('getBucketLifecycle API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should return NoSuchLifecycleConfiguration error if ' +
    'bucket has no lifecycle', done => {
        const lifecycleRequest = getLifecycleRequest(bucketName);
        bucketGetLifecycle(authInfo, lifecycleRequest, log, err => {
            assert.strictEqual(err.NoSuchLifecycleConfiguration, true);
            done();
        });
    });

    describe('after bucket lifecycle has been put', () => {
        beforeEach(done => {
            const putRequest =
                getLifecycleRequest(bucketName, getLifecycleXml());
            bucketPutLifecycle(authInfo, putRequest, log, err => {
                assert.equal(err, null);
                done();
            });
        });

        it('should return lifecycle XML', done => {
            const getRequest = getLifecycleRequest(bucketName);
            bucketGetLifecycle(authInfo, getRequest, log, (err, res) => {
                assert.equal(err, null);
                const expectedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
                    `${getLifecycleXml()}`;
                assert.deepStrictEqual(expectedXML, res);
                done();
            });
        });
    });
});
