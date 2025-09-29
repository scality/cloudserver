const { fromIni } = require('@aws-sdk/credential-providers');
const fs = require('fs');
const path = require('path');
const { config } = require('../../../../../lib/Config');
const https = require('https');
const http = require('http');

function getAwsCredentials(profile, credFile = '/.aws/credentials') {
    const filename = path.join(process.env.HOME, credFile);

    try {
        fs.statSync(filename);
    } catch {
        const msg = `AWS credential file does not exist: ${filename}`;
        throw new Error(msg);
    }

    return fromIni({ profile, filepath: filename });
}

function getRealAwsConfig(location) {
    const { awsEndpoint, gcpEndpoint, credentialsProfile,
        credentials: locCredentials, bucketName, mpuBucketName, pathStyle } =
        config.locationConstraints[location].details;
    const useHTTPS = config.locationConstraints[location].details.https;
    const proto = useHTTPS ? 'https' : 'http';
    const params = {
        region: 'us-east-1',
        endpoint: gcpEndpoint ?
            `${proto}://${gcpEndpoint}` : `${proto}://${awsEndpoint}`,
    };
    if (config.locationConstraints[location].type === 'gcp') {
        params.mainBucket = bucketName;
        params.mpuBucket = mpuBucketName;
    }
    if (useHTTPS) {
        params.requestHandler = {
            httpsAgent: new https.Agent({ keepAlive: true }),
        };
    } else {
        params.requestHandler = {
            httpAgent: new http.Agent({ keepAlive: true }),
        };
    }
    if (credentialsProfile) {
        const credentials = getAwsCredentials(credentialsProfile,
            '/.aws/credentials');
        params.credentials = credentials;
        return params;
    }
    if (pathStyle) {
        params.forcePathStyle = true;
    }
    // sslEnabled not needed in v3, handled by endpoint protocol
    params.credentials = {
        accessKeyId: locCredentials.accessKey,
        secretAccessKey: locCredentials.secretKey,
    };
    return params;
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
