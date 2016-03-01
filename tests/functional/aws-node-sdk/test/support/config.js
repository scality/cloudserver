import fs from 'fs';
import path from 'path';
import AWS from 'aws-sdk';
import memCredentials from '../../lib/json/mem_credentials.json';

const DEFAULT_GLOBAL_OPTIONS = {
    logger: process.stdout,
    apiVersions: { s3: '2006-03-01' },
};
const DEFAULT_MEM_OPTIONS = {
    endpoint: 'http://localhost:8000',
    sslEnabled: false,
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

function _getAwsCredentials(profile) {
    const filename = path.join(process.env.HOME, '/.aws/scality');

    try {
        fs.statSync(filename);
    } catch (e) {
        const msg = `AWS credential file is not existing: ${filename}`;
        throw new Error(msg);
    }

    return new AWS.SharedIniFileCredentials({ profile, filename });
}

function _getMemConfig(profile, config) {
    let memConfig;
    const credentials = _getMemCredentials(profile);

    memConfig = Object.assign({}
        , DEFAULT_GLOBAL_OPTIONS, DEFAULT_MEM_OPTIONS
        , { credentials }, config);

    if (process.env.IP) {
        memConfig.endpoint = `http://${process.env.IP}:8000`;
    }

    return memConfig;
}

function _getAwsConfig(profile, config) {
    let awsConfig;
    const credentials = _getAwsCredentials(profile);

    awsConfig = Object.assign({}
        , DEFAULT_GLOBAL_OPTIONS, DEFAULT_AWS_OPTIONS
        , { credentials }, config);

    return awsConfig;
}

export default function getConfig(profile = 'default', config = {}) {
    const fn = process.env.AWS_ON_AIR && process.env.AWS_ON_AIR === 'true'
        ? _getAwsConfig : _getMemConfig;

    return fn.apply(this, [profile, config]);
}
