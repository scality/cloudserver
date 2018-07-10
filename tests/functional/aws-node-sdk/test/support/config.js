const https = require('https');
const AWS = require('aws-sdk');

const memCredentials = require('../../lib/json/mem_credentials.json');
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
    apiVersions: { s3: '2006-03-01' },
    signatureCache: false,
    sslEnabled: ssl !== undefined,
};
const DEFAULT_MEM_OPTIONS = {
    endpoint: `${transport}://127.0.0.1:8000`,
    s3ForcePathStyle: true,
};
const DEFAULT_AWS_OPTIONS = {};

if (!memCredentials || Object.is(memCredentials, {})) {
    throw new Error('Credential info is missing in mem_credentials.json');
}

function _getMemCredentials(profile) {
    const credentials = memCredentials[profile] || memCredentials.default;

    const accessKeyId = credentials.accessKey;
    const secretAccessKey = credentials.secretKey;

    return new AWS.Credentials(accessKeyId, secretAccessKey);
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
    const credentials = getAwsCredentials(profile, '/.aws/scality');

    const awsConfig = Object.assign({}
        , DEFAULT_GLOBAL_OPTIONS, DEFAULT_AWS_OPTIONS
        , { credentials }, config);

    return awsConfig;
}

function getConfig(profile = 'default', config = {}) {
    const fn = process.env.AWS_ON_AIR && process.env.AWS_ON_AIR === 'true'
        ? _getAwsConfig : _getMemConfig;

    return fn.apply(this, [profile, config]);
}

module.exports = getConfig;
