const assert = require('assert');
const parseLC = require('../../../lib/data/locationConstraintParser');
const inMemory = require('../../../lib/data/in_memory/backend').backend;
const DataFileInterface = require('../../../lib/data/file/backend');

const clients = parseLC();

describe('locationConstraintParser', () => {
    it('should return object containing mem object', () => {
        assert.notEqual(Object.keys(clients).indexOf('mem'), -1);
        assert.strictEqual(typeof clients.mem, 'object');
        assert.deepEqual(clients.mem, inMemory);
    });
    it('should return object containing file object', () => {
        assert.notEqual(Object.keys(clients).indexOf('file'), -1);
        assert(clients.file instanceof DataFileInterface);
    });
});
