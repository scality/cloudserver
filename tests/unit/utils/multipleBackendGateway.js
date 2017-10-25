const assert = require('assert');
const { checkExternalBackend } = require('../../../lib/data/external/utils');
const awsLocations = [
    'aws-test',
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
        'aws-test': {
            healthcheck: (location, cb) => cb(null, { 'aws-test': status }),
        },
    };
}

describe('Testing _checkExternalBackend', function describeF() {
    this.timeout(50000);
    beforeEach(done => {
        const clients = getClients(true);
        return checkExternalBackend(clients, awsLocations, 'aws_s3',
        externalBackendHealthCheckInterval, done);
    });
    it('should not refresh response before externalBackendHealthCheckInterval',
    done => {
        const clients = getClients(false);
        return checkExternalBackend(clients, awsLocations, 'aws_s3',
        externalBackendHealthCheckInterval, (err, res) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(res['aws-test'], statusSuccess);
            return done();
        });
    });

    it('should refresh response after externalBackendHealthCheckInterval',
    done => {
        const clients = getClients(false);
        setTimeout(() => {
            checkExternalBackend(clients, awsLocations, 'aws_s3',
            externalBackendHealthCheckInterval, (err, res) => {
                if (err) {
                    return done(err);
                }
                assert.strictEqual(res['aws-test'], statusFailure);
                return done();
            });
        }, externalBackendHealthCheckInterval + 1);
    });
});
