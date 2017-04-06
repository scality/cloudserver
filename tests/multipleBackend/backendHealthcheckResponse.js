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
    it('should return no error', done => {
        clientCheck(log, err => {
            assert.strictEqual(err, null,
                `Expected success but got error ${err}`);
            done();
        });
    });
    it('should return result for every location constraint in ' +
    'locationConfig', done => {
        clientCheck(log, (err, results) => {
            locConstraints.forEach(constraint => {
                assert.notEqual(Object.keys(results).
                    indexOf(constraint), -1);
            });
            done();
        });
    });
});

