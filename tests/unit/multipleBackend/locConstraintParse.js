const assert = require('assert');
const parseLC = require('../../../lib/data/locationConstraintParser');
const inMemory = require('../../../lib/data/in_memory/backend').backend;
const DataFileInterface = require('../../../lib/data/file/backend');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const awsLocation = 'awsbackend';
const clients = parseLC();

describe('locationConstraintParser', () => {
    it('should return object containing mem object', () => {
        assert.notEqual(Object.keys(clients).indexOf(memLocation), -1);
        assert.strictEqual(typeof clients[memLocation], 'object');
        assert.deepEqual(clients[memLocation], inMemory);
    });
    it('should return object containing file object', () => {
        assert.notEqual(Object.keys(clients).indexOf(fileLocation), -1);
        assert(clients[fileLocation] instanceof DataFileInterface);
    });
    it('should return object containing AWS object', () => {
        assert.notEqual(Object.keys(clients).indexOf(awsLocation), -1);
        assert.strictEqual(typeof clients[awsLocation], 'object');
    });
});
