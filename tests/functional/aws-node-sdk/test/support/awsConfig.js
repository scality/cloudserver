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

    // Parse the INI file manually for synchronous access
    const content = fs.readFileSync(filename, 'utf-8');
    const profileMatch = content.match(new RegExp(`\\[${profile}\\][\\s\\S]*?(?=\\n\\[|$)`));

    if (!profileMatch) {
        throw new Error(`Profile "${profile}" not found in ${filename}`);
    }

    const accessKeyMatch = profileMatch[0].match(/aws_access_key_id\s*=\s*(.+)/);
    const secretKeyMatch = profileMatch[0].match(/aws_secret_access_key\s*=\s*(.+)/);

    if (!accessKeyMatch || !secretKeyMatch) {
        throw new Error(`Missing credentials in profile "${profile}"`);
    }

    return {
        accessKeyId: accessKeyMatch[1].trim(),
        secretAccessKey: secretKeyMatch[1].trim(),
    };
}

function getRealAwsConfig(location) {
    const {
        awsEndpoint,
        gcpEndpoint,
        credentialsProfile,
        credentials: locCredentials,
        bucketName,
        mpuBucketName,
        pathStyle,
    } = config.locationConstraints[location].details;
    const useHTTPS = config.locationConstraints[location].details.https;
    const proto = useHTTPS ? 'https' : 'http';
    const isGcp = config.locationConstraints[location].type === 'gcp';
    const params = {
        region: 'us-east-1',
        endpoint: gcpEndpoint ? `${proto}://${gcpEndpoint}` : `${proto}://${awsEndpoint}`,
    };

    if (isGcp) {
        params.disableS3ExpressSessionAuth = true;
        params.useGlobalEndpoint = false;
        params.s3DisableBodySigning = true;
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

    if (pathStyle) {
        params.forcePathStyle = true;
    }

    if (!useHTTPS) {
        params.sslEnabled = false;
    }

    if (credentialsProfile) {
        const credentials = getAwsCredentials(credentialsProfile, '/.aws/credentials');
        params.credentials = credentials;

        if (isGcp) {
            return {
                s3Params: params,
                bucketName,
                mpuBucket: mpuBucketName || bucketName,
                credentials: {
                    // For raw HTTP requests (GCP format)
                    accessKey: credentials.accessKeyId,
                    secretKey: credentials.secretAccessKey,
                },
            };
        }
        return params;
    }
    params.credentials = {
        accessKeyId: locCredentials.accessKey,
        secretAccessKey: locCredentials.secretKey,
    };

    // For GCP with plain credentials, return nested structure
    if (isGcp) {
        return {
            s3Params: {
                ...params,
                credentials: {
                    accessKeyId: locCredentials.accessKey,
                    secretAccessKey: locCredentials.secretKey,
                },
            },
            bucketName,
            mpuBucket: mpuBucketName || bucketName,
            credentials: {
                accessKey: locCredentials.accessKey,
                secretKey: locCredentials.secretKey,
            },
        };
    }

    return params;
}

module.exports = {
    getRealAwsConfig,
    getAwsCredentials,
};
