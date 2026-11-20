const assert = require('assert');
const sinon = require('sinon');

const notificationDestinationCreate = require('../../../lib/api/notificationDestinationCreate');
const notificationDestinationGet = require('../../../lib/api/notificationDestinationGet');
const notificationDestinationList = require('../../../lib/api/notificationDestinationList');
const notificationDestinationUpdate = require('../../../lib/api/notificationDestinationUpdate');
const notificationDestinationDelete = require('../../../lib/api/notificationDestinationDelete');
const { DummyRequestLogger } = require('../helpers');
const {
    accountAuthInfo,
    makeRequest,
    listRequest,
    call,
    useMemRecordStore,
} = require('../utils/notificationDestinationHelpers');

const body =
    '<NotificationDestination>' +
    '<Type>kafka</Type>' +
    '<Connection>' +
    '<Brokers><Broker>broker1.example.com:9093</Broker></Brokers><Topic>bucket-events</Topic>' +
    '</Connection>' +
    '<Auth><Type>none</Type></Auth>' +
    '</NotificationDestination>';

// A callback that throws, such as an assertion of a test or a response
// writer that fails, must not get a second call with its own error.
describe('notification destination handlers with a callback that throws', () => {
    let log;
    let restoreRecordStore;

    beforeEach(async () => {
        restoreRecordStore = useMemRecordStore();
        log = new DummyRequestLogger();
        // Get, Update and Delete read this destination.
        const [err] = await call(
            notificationDestinationCreate,
            accountAuthInfo,
            makeRequest('PUT', 'orders', body),
            log,
        );
        assert.ifError(err);
    });

    afterEach(() => {
        restoreRecordStore();
        sinon.restore();
    });

    [
        [notificationDestinationCreate, () => makeRequest('PUT', 'payments', body)],
        [notificationDestinationGet, () => makeRequest('GET', 'orders')],
        [notificationDestinationList, () => listRequest()],
        [notificationDestinationUpdate, () => makeRequest('POST', 'orders', body)],
        [notificationDestinationDelete, () => makeRequest('DELETE', 'orders')],
    ].forEach(([handler, request]) => {
        it(`should call the callback of ${handler.name} once`, async () => {
            const thrown = new Error('callback failed');
            const callback = sinon.stub().throws(thrown);
            await assert.rejects(handler(accountAuthInfo, request(), log, callback), err => err === thrown);
            sinon.assert.calledOnce(callback);
            assert.strictEqual(callback.firstCall.args[0], null, 'the handler should succeed');
        });
    });
});
