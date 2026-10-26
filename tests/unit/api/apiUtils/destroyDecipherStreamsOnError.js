const assert = require('assert');
const { EventEmitter } = require('events');
const sinon = require('sinon');

const destroyDecipherStreamsOnError =
    require('../../../../lib/api/apiUtils/object/destroyDecipherStreamsOnError');

describe('destroyDecipherStreamsOnError', () => {
    let locations;
    let request;
    let response;

    beforeEach(() => {
        locations = [
            { decipherStream: { destroy: sinon.spy() } },
            { decipherStream: { destroy: sinon.spy() } },
            // KMS call failed or skipped for this part
            {},
        ];
        request = { kmsDecipherLocations: locations };
        response = new EventEmitter();
        response.statusCode = 200;
        response.writableFinished = false;
    });

    const assertDestroyed = destroyed => locations
        .filter(location => location.decipherStream)
        .forEach(location => assert.strictEqual(
            location.decipherStream.destroy.calledOnce, destroyed));

    it('should do nothing without decipher locations', () => {
        destroyDecipherStreamsOnError(new Error(), {}, response);
        assert.strictEqual(response.listenerCount('close'), 0);
    });

    it('should destroy decipher streams on objectGet error', () => {
        destroyDecipherStreamsOnError(new Error(), request, response);
        assertDestroyed(true);
        assert.strictEqual(response.listenerCount('close'), 0);
    });

    it('should destroy decipher streams if the response is already closed',
        () => {
            response.isclosed = true;
            destroyDecipherStreamsOnError(null, request, response);
            assertDestroyed(true);
            assert.strictEqual(response.listenerCount('close'), 0);
        });

    it('should not destroy decipher streams once a successful response ' +
    'is fully sent', () => {
        // Because they are fully consumed by the response
        destroyDecipherStreamsOnError(null, request, response);
        assertDestroyed(false);
        response.writableFinished = true;
        response.emit('close');
        assertDestroyed(false);
    });

    it('should destroy decipher streams if the response is closed before ' +
    'being fully sent', () => {
        destroyDecipherStreamsOnError(null, request, response);
        response.emit('close');
        assertDestroyed(true);
    });

    it('should destroy decipher streams if an error response is sent', () => {
        destroyDecipherStreamsOnError(null, request, response);
        response.statusCode = 500;
        response.writableFinished = true;
        response.emit('close');
        assertDestroyed(true);
    });
});
