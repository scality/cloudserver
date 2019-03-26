'use strict'; // eslint-disable-line strict
const assert = require('assert');
const { errors } = require('arsenal');
const DummyRequestLogger = require('../unit/helpers').DummyRequestLogger;
const clientCheck
    = require('../../lib/utilities/healthcheckHandler').clientCheck;
const { config } = require('../../lib/Config');
const {
    getAzureClient,
    azureLocationNonExistContainer,
    getAzureContainerName,
} = require('../functional/aws-node-sdk/test/multipleBackend/utils');

const log = new DummyRequestLogger();
const locConstraints = Object.keys(config.locationConstraints);
const azureClient = getAzureClient();

describe('Healthcheck response', () => {
    test('should return result for every location constraint in ' +
    'locationConfig and every external locations with flightCheckOnStartUp ' +
    'set to true', done => {
        clientCheck(true, log, (err, results) => {
            const resultKeys = Object.keys(results);
            locConstraints.forEach(constraint => {
                expect(resultKeys.includes(constraint)).toBeTruthy();
            });
            done();
        });
    });
    test('should return no error with flightCheckOnStartUp set to false', done => {
        clientCheck(false, log, err => {
            expect(err).toBe(null);
            done();
        });
    });
    test('should return result for every location constraint in ' +
    'locationConfig and at least one of every external locations with ' +
    'flightCheckOnStartUp set to false', done => {
        clientCheck(false, log, (err, results) => {
            expect(results.length).not.toBe(locConstraints.length);
            locConstraints.forEach(constraint => {
                if (Object.keys(results).indexOf(constraint) === -1) {
                    const locationType = config
                        .locationConstraints[constraint].type;
                    expect(Object.keys(results).some(result =>
                      config.locationConstraints[result].type
                        === locationType)).toBeTruthy();
                }
            });
            done();
        });
    });

    describe('Azure container creation', () => {
        const containerName =
            getAzureContainerName(azureLocationNonExistContainer);

        beforeEach(done => {
            azureClient.deleteContainerIfExists(containerName, done);
        });

        afterEach(done => {
            azureClient.deleteContainerIfExists(containerName, done);
        });

        test('should create an azure location\'s container if it is missing ' +
        'and the check is a flightCheckOnStartUp', done => {
            clientCheck(true, log, (err, results) => {
                const azureLocationNonExistContainerError =
                    results[azureLocationNonExistContainer].error;
                if (err) {
                    expect(err).toBe(errors.InternalError);
                    expect(azureLocationNonExistContainerError.startsWith(
                        'The specified container is being deleted.')).toBeTruthy();
                    return done();
                }
                return azureClient.getContainerMetadata(containerName,
                    (err, azureResult) => {
                        expect(err).toBe(null);
                        expect(azureResult.name).toBe(containerName);
                        return done();
                    });
            });
        });

        test('should not create an azure location\'s container even if it is ' +
        'missing if the check is not a flightCheckOnStartUp', done => {
            clientCheck(false, log, err => {
                expect(err).toBe(null);
                return azureClient.getContainerMetadata(containerName, err => {
                    expect(err).toBeTruthy();
                    expect(err.code).toBe('NotFound');
                    return done();
                });
            });
        });
    });
});
