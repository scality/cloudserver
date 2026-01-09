const https = require('https');
const http = require('http');
const { NodeHttpHandler } = require('@smithy/node-http-handler');

const { getCredentials } = require('./credentials');
const { getAwsCredentials } = require('./awsConfig');
const conf = require('../../../../../lib/Config').config;

const transport = conf.https ? 'https' : 'http';

const ssl = conf.https;
let httpOptions;
if (ssl && ssl.ca) {
    httpOptions = {
        agent: new https.Agent({
            ca: [ssl.ca],
        }),
    };
}

const DEFAULT_GLOBAL_OPTIONS = {
    httpOptions,
};

const DEFAULT_MEM_OPTIONS = {
    endpoint: `${transport}://127.0.0.1:8000`,
    port: 8000,
    forcePathStyle: true,
    region: 'us-east-1', 
    maxAttempts: 3,
    requestHandler: new NodeHttpHandler({
        connectionTimeout: 5000,
        requestTimeout: 5000,
        httpAgent: new (ssl ? https : http).Agent({
            maxSockets: 200,
            keepAlive: true,
            keepAliveMsecs: 1000,
        }),
    }),
};

const DEFAULT_AWS_OPTIONS = {
    region: 'us-east-1',
    maxAttempts: 3,
    requestHandler: new NodeHttpHandler({
        connectionTimeout: 5000,
        socketTimeout: 5000,
        httpAgent: new https.Agent({
            maxSockets: 200,
            keepAlive: true,
            keepAliveMsecs: 1000,
        }),
    }),
};

function _getMemCredentials(profile) {
    const { accessKeyId, secretAccessKey } = getCredentials(profile);
    return {
        accessKeyId,
        secretAccessKey,
    };
}

function _getMemConfig(profile, config) {
    const credentials = _getMemCredentials(profile);

    const memConfig = Object.assign({}
        , DEFAULT_GLOBAL_OPTIONS, DEFAULT_MEM_OPTIONS
        , { credentials }, config);

    if (process.env.IP) {
        memConfig.endpoint = `${transport}://${process.env.IP}:8000`;
    }

    return memConfig;
}

function _getAwsConfig(profile, config) {
    const credentials = getAwsCredentials(profile);

    const awsConfig = Object.assign({}
        , DEFAULT_GLOBAL_OPTIONS, DEFAULT_AWS_OPTIONS
        , { credentials }, config);

    return awsConfig;
}

function getConfig(profile, config) {
    if (process.env.AWS_ON_AIR) {
        return _getAwsConfig(profile, config);
    }
    return _getMemConfig(profile, config);
}

module.exports = getConfig;
