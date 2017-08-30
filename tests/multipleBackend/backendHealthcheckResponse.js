'use strict'; // eslint-disable-line strict
const assert = require('assert');
const DummyRequestLogger = require('../unit/helpers').DummyRequestLogger;
const clientCheck
    = require('../../lib/utilities/healthcheckHandler').clientCheck;
const { config } = require('../../lib/Config');

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
    'locationConfig and at least one of every external locations', done => {
        clientCheck(log, (err, results) => {
            locConstraints.forEach(constraint => {
                if (Object.keys(results).indexOf(constraint) === -1) {
                    const locationType = config.locationConstraints
                    [constraint].type;
                    assert(Object.keys(results).some(result =>
                      config.locationConstraints[result].type
                        === locationType));
                }
            });
            done();
        });
    });
});
