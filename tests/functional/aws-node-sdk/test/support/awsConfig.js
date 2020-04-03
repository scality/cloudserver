const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');
const { config } = require('../../../../../lib/Config');
const https = require('https');
const http = require('http');
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

function getRealAwsConfig(location) {
    const { awsEndpoint, gcpEndpoint, credentialsProfile,
        credentials: locCredentials, bucketName, mpuBucketName, pathStyle } =
        config.locationConstraints[location].details;
    const useHTTPS = config.locationConstraints[location].details.https;
    const proto = useHTTPS ? 'https' : 'http';
    const params = {
        endpoint: gcpEndpoint ?
            `${proto}://${gcpEndpoint}` : `${proto}://${awsEndpoint}`,
        signatureVersion: 'v4',
    };
    if (config.locationConstraints[location].type === 'gcp') {
        params.mainBucket = bucketName;
        params.mpuBucket = mpuBucketName;
    }
    if (useHTTPS) {
        params.httpOptions = {
            agent: new https.Agent({ keepAlive: true }),
        };
    } else {
        params.httpOptions = {
            agent: new http.Agent({ keepAlive: true }),
        };
    }
    if (credentialsProfile) {
        const credentials = getAwsCredentials(credentialsProfile,
            '/.aws/credentials');
        params.credentials = credentials;
        return params;
    }
    if (pathStyle) {
        params.s3ForcePathStyle = true;
    }
    if (!useHTTPS) {
        params.sslEnabled = false;
    }
    params.accessKeyId = locCredentials.accessKey;
    params.secretAccessKey = locCredentials.secretKey;
    return params;
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
