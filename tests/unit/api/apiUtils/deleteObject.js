const assert = require('assert');
const { Logger } = require('werelogs');
const { errors, storage } = require('arsenal');
const helpers = require('../../helpers');
const { ds, backend } = storage.data.inMemory.datastore;
const { dataDelete } =
    require('../../../../lib/api/apiUtils/object/deleteObject');
const log = new Logger('_').newRequestLogger();

describe('dataDelete utility', () => {
    const key = 1;
    const value = Buffer.from('_');

    beforeEach(() => helpers.cleanup());

    describe('success case', () => {
        beforeEach(done => {
            ds[key] = { value };
            dataDelete({ key }, log, done);
        });

        it('should delete the key', () => {
            assert.strictEqual(ds[key], undefined);
        });
    });

    describe('error case', () => {
        beforeEach(done => {
            ds[key] = { value };
            backend.errors.delete = errors.InternalError;
            dataDelete({ key }, log, err => {
                delete backend.errors.delete;
                assert.deepStrictEqual(err, errors.InternalError);
                done();
            });
        });

        it('should not delete the key', () => {
            assert.deepStrictEqual(ds[key], { value });
        });
    });
});
