const assert = require('assert');
const arsenal = require('arsenal');
const { checkExternalBackend } = arsenal.storage.data.external.backendUtils;
const awsLocations = [
    'awsbackend',
];

const statusSuccess = {
    versioningStatus: 'Enabled',
    message: 'Congrats! You own the bucket',
};

const statusFailure = {
    versioningStatus: 'Suspended',
    error: 'Versioning must be enabled',
    external: true,
};

const externalBackendHealthCheckInterval = 10000;

function getClients(isSuccess) {
    const status = isSuccess ? statusSuccess : statusFailure;
    return {
        awsbackend: {
            healthcheck: (location, cb) => cb(null, { awsbackend: status }),
        },
    };
}

describe('Testing _checkExternalBackend', () => {
    this.timeout(50000);
    beforeEach(done => {
        const clients = getClients(true);
        return checkExternalBackend(clients, awsLocations, 'aws_s3', false,
        externalBackendHealthCheckInterval, done);
    });
    test(
        'should not refresh response before externalBackendHealthCheckInterval',
        done => {
            const clients = getClients(false);
            return checkExternalBackend(clients, awsLocations, 'aws_s3',
            false, externalBackendHealthCheckInterval, (err, res) => {
                if (err) {
                    return done(err);
                }
                expect(res[0].awsbackend).toBe(statusSuccess);
                return done();
            });
        }
    );

    test(
        'should refresh response after externalBackendHealthCheckInterval',
        done => {
            const clients = getClients(false);
            setTimeout(() => {
                checkExternalBackend(clients, awsLocations, 'aws_s3',
                false, externalBackendHealthCheckInterval, (err, res) => {
                    if (err) {
                        return done(err);
                    }
                    expect(res[0].awsbackend).toBe(statusFailure);
                    return done();
                });
            }, externalBackendHealthCheckInterval + 1);
        }
    );
});
