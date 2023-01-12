const AWS = require('aws-sdk');

const { Service } = AWS;

// TODO: move lifecycle client. 
// CloudServer uses it for testing only. Backbeat uses it as a client.

AWS.apiLoader.services.lifecycle = {};
const serviceIdentifier = 'lifecycle';
const versions = ['2023-01-01'];
const features = {
    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },
};

const LifecycleClient = Service.defineService(
    serviceIdentifier,
    versions,
    features,
);

Object.defineProperty(AWS.apiLoader.services.lifecycle, '2023-01-01', {
    get: function get() {
        const model = require('./lifecycle-2023-01-01.api.json'); // eslint-disable-line
        return model;
    },
    enumerable: true,
    configurable: true,
});

module.exports = LifecycleClient;
