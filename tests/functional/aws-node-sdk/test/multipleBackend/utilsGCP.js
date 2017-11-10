const gcp = require('google-cloud');

const { config } = require('../../../../../lib/Config');

const gcpLocation = 'gcp-test';

const utils = {};

utils.uniqName = name => `${name}${new Date().getTime()}`;

utils.getGcpClient = () => {
    let isTestingGcp;
    let gcpCredentials;
    let gcpClient;

    if (process.env[`${gcpLocation}_GCP_CRED`]) {
        isTestingGcp = true;
        gcpCredentials = process.env[`${gcpLocation}_GCP_CRED`];
    } else if (config.locationConstraints[gcpLocation] &&
          config.locationConstraints[gcpLocation].details &&
          config.locationConstraints[gcpLocation].details.credentialsEnv) {
        const locationObj = config.locationConstraints[gcpLocation];
        isTestingGcp = true;
        gcpCredentials =
            process.env[`${locationObj.details.credentialsEnv}`] ?
            process.env[`${locationObj.details.credentialsEnv}`] :
            locationObj.details.keyFilename;
    } else {
        isTestingGcp = false;
    }

    if (isTestingGcp) {
        gcpClient = gcp.storage({
            keyFilename: gcpCredentials,
        });
    }
    return gcpClient;
};

utils.getGcpBucketName = () => {
    let gcpBucketName;

    if (config.locationConstraints[gcpLocation] &&
    config.locationConstraints[gcpLocation].details &&
    config.locationConstraints[gcpLocation].details.gcpBucketName) {
        gcpBucketName =
            config.locationConstraints[gcpLocation].details.gcpBucketName;
    }
    return gcpBucketName;
};

utils.getGcpKeys = () => {
    const keys = [
        {
            describe: 'empty',
            name: `somekey-${Date.now()}`,
            body: '',
            MD5: 'd41d8cd98f00b204e9800998ecf8427e',
        },
        {
            describe: 'normal',
            name: `somekey-${Date.now()}`,
            body: Buffer.from('I am a body', 'utf8'),
            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
        },
        {
            describe: 'big',
            name: `bigkey-${Date.now()}`,
            body: new Buffer(10485760),
            MD5: 'f1c9645dbc14efddc7d8a322685f26eb',
        },
    ];
    return keys;
};

utils.convertMD5 = contentMD5 =>
    Buffer.from(contentMD5, 'base64').toString('hex');

module.exports = utils;
