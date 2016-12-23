import assert from 'assert';

describe('Config', () => {
    it('should load default config.json without errors', done => {
        require('../../lib/Config');
        done();
    });

    const cfg = {
        port: 8000,
        regions: {
            'ap-northeast-1': ['s3.ap-northeast-1.amazonaws.com'],
            'ap-southeast-1': ['s3.ap-southeast-1.amazonaws.com'],
            'ap-southeast-2': ['s3.ap-southeast-2.amazonaws.com'],
            'eu-central-1': ['s3.eu-central-1.amazonaws.com',
                         's3.eu.central-1.amazonaws.com'],
            'eu-west-1': ['s3.eu-west-1.amazonaws.com'],
            'sa-east-1': ['s3.sa-east-1.amazonaws.com'],
            'us-east-1': ['s3.amazonaws.com',
                      's3-external-1.amazonaws.com',
                      's3.us-east-1.amazonaws.com'],
            'us-west-1': ['s3.us-west-1.amazonaws.com'],
            'us-west-2': ['s3-us-west-2.amazonaws.com'],
            'us-gov-west-1': ['s3-us-gov-west-1.amazonaws.com',
                          's3-fips-us-gov-west-1.amazonaws.com'],
            'localregion': ['localhost'],
        },
        sproxyd: {
            bootstrap: ['localhost:8181'],
        },
        bucketd: {
            bootstrap: ['localhost'],
        },
        vaultd: {
            host: 'localhost',
            port: 8500,
        },
        clusters: 10,
        log: {
            logLevel: 'info',
            dumpLevel: 'error',
        },
    };

    it('should load config with well-formed ' +
      'user-defined predicates', done => {
        cfg.predicates = [{
            eventInfo: {
                eventName: 'ObjectCreated:Put',
                bucket: 'foo',
            },
            path(event, context, callback) {
                return callback();
            },
        }];
        const Config = require('../../lib/Config').Config;
        // eslint-disable-next-line no-new
        new Config(cfg);
        done();
    });

    it('should throw error when predicates are ' +
     'not well-formed', done => {
        cfg.predicates = [{
            eventInfo: {
                eventName: 'ObjectCreated:Put',
                bucket: 'foo',
            },
            path: '/does/not/exist',
        }];
        const Config = require('../../lib/Config').Config;
        assert.throws(() => {
            // eslint-disable-next-line no-new
            new Config(cfg);
        });
        done();
    });
});
