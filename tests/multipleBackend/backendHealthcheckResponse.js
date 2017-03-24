'use strict'; // eslint-disable-line strict
const assert = require('assert');
require('babel-core/register');
const DummyRequestLogger = require('../unit/helpers').DummyRequestLogger;
const clientCheck =
    require('../../lib/utilities/healthcheckHandler').clientCheck;
const config = require('../../lib/Config').default;

const log = new DummyRequestLogger();
const locConstraints = Object.keys(config.locationConstraints);

describe('Healthcheck response', () => {
    it('should return object containing dataBackends key', done => {
        clientCheck(log, (err, results) => {
            assert.strictEqual(err, null, `Unexpected error ${err}`);
            assert.notEqual(
                Object.keys(results).indexOf('dataBackends'), -1);
            done();
        });
    });
    it('should return result for every location constraint in ' +
    'locationConfig', done => {
        clientCheck(log, (err, results) => {
            assert.strictEqual(err, null, `Unexpected error ${err}`);
            locConstraints.forEach(constraint => {
                assert.notEqual(Object.keys(results.dataBackends).
                    indexOf(constraint), -1);
            });
            done();
        });
    });
});

