import assert from 'assert';
import parseLC from '../../../lib/data/locationConstraintParser';
import inMemory from '../../../lib/data/in_memory/backend';
import DataFileInterface from '../../../lib/data/file/backend';


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
