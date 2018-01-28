const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const { config } = require('../../../../../lib/Config');
const https = require('https');

function getAwsCredentials(profile, credFile) {
    const filename = path.join(process.env.HOME, credFile);

    try {
        fs.statSync(filename);
    } catch (e) {
        const msg = `AWS credential file does not exist: ${filename}`;
        throw new Error(msg);
    }

    return new AWS.SharedIniFileCredentials({ profile, filename });
}

function getRealAwsConfig(awsLocation) {
    const { awsEndpoint, gcpEndpoint,
        credentialsProfile, credentials: locCredentials } =
        config.locationConstraints[awsLocation].details;
    const params = {
        endpoint: gcpEndpoint ?
            `https://${gcpEndpoint}` : `https://${awsEndpoint}`,
        signatureVersion: 'v4',
    };
    if (credentialsProfile) {
        const credentials = getAwsCredentials(credentialsProfile,
            '/.aws/credentials');
        params.credentials = credentials;
        return params;
    }
    params.httpOptions = {
        agent: new https.Agent({
            keepAlive: true,
        }),
    };
    params.accessKeyId = locCredentials.accessKey;
    params.secretAccessKey = locCredentials.secretKey;
    return params;
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
