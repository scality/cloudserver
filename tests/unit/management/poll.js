const assert = require('assert');
const sinon = require('sinon');

const request = require('../../../lib/utilities/request');
const { loadRemoteOverlay } = require('../../../lib/management/poll');
const { DummyRequestLogger } = require('../helpers');

describe('management poll loadRemoteOverlay', () => {
    const managementEndpoint = 'https://management.example.com';
    const instanceId = 'instance-id';
    const remoteToken = 'remote-token';
    const log = new DummyRequestLogger();

    afterEach(() => {
        sinon.restore();
    });

    it('should forward the werelogs request uids under the ' + 'x-scal-request-uids header', done => {
        const getStub = sinon.stub(request, 'get').callsFake((url, opts, cb) => cb(null, { statusCode: 200 }, {}));

        loadRemoteOverlay(managementEndpoint, instanceId, remoteToken, {}, log, err => {
            assert.ifError(err);
            assert(getStub.calledOnce);
            const opts = getStub.firstCall.args[1];
            assert.strictEqual(opts.headers['x-scal-request-uids'], log.getSerializedUids());
            assert.strictEqual(opts.headers['x-scal-request-id'], undefined);
            done();
        });
    });
});
