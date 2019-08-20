const assert = require('assert');
const { Logger } = require('werelogs');
const { errors } = require('arsenal');
const helpers = require('../../helpers');
const { ds, backend } = require('../../../../lib/data/in_memory/backend');
const { dataDelete } =
    require('../../../../lib/api/apiUtils/object/deleteObject');

describe('dataDelete utility', () => {
    let log;
    const key = 1;
    const key2 = 2;
    const value = Buffer.from('_');

    beforeEach(() => {
        helpers.cleanup();
        // Batch delete calls log.end();
        log = new Logger('_').newRequestLogger();
    });

    it('should check that the locations are an array', done => {
        const locations = {};
        dataDelete(locations, 'PUT', log, done);
    });

    it('should check that the locations are an array > 0 in length', done => {
        const locations = [];
        dataDelete(locations, 'PUT', log, done);
    });

    describe('success case', () => {
        describe('with a single location', () => {
            beforeEach(done => {
                ds[key] = { value };
                const locations = [{ key }];
                dataDelete(locations, 'PUT', log, done);
            });

            it('should delete the key', () => {
                assert.strictEqual(ds[key], undefined);
            });
        });

        describe('with multiple locations', () => {
            beforeEach(done => {
                ds[key] = { value };
                ds[key2] = { value };
                const locations = [{ key }, { key: key2 }];
                dataDelete(locations, 'PUT', log, done);
            });

            it('should delete each key', () => {
                assert.strictEqual(ds[key], undefined);
                assert.strictEqual(ds[key2], undefined);
            });
        });
    });

    describe('error case', () => {
        describe('with a single location', () => {
            beforeEach(done => {
                ds[key] = { value };
                const locations = [{ key }];
                backend.errors.delete = errors.InternalError;
                dataDelete(locations, 'PUT', log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
            });

            afterEach(() => delete backend.errors.delete);

            it('should not delete the key', () => {
                assert.deepStrictEqual(ds[key], { value });
            });
        });

        describe('with multiple locations', () => {
            beforeEach(done => {
                ds[key] = { value };
                ds[key2] = { value };
                const locations = [{ key }, { key: key2 }];
                backend.errors.delete = errors.InternalError;
                dataDelete(locations, 'PUT', log, err => {
                    assert.deepStrictEqual(err, errors.InternalError);
                    done();
                });
            });

            afterEach(() => delete backend.errors.delete);

            it('should delete each key', () => {
                assert.deepStrictEqual(ds[key], { value });
                assert.deepStrictEqual(ds[key2], { value });
            });
        });
    });
});
