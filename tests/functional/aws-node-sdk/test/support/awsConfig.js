const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

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

function getRealAwsConfig(profile) {
    const credentials = getAwsCredentials(profile, '/.aws/credentials');
    const realAwsConfig = { credentials, signatureVersion: 'v4' };

    return realAwsConfig;
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
