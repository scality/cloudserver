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
    const isGcp = config.locationConstraints[location].type === 'gcp';
    const params = {
        region: 'us-east-1',
        endpoint: gcpEndpoint ?
            `${proto}://${gcpEndpoint}` : `${proto}://${awsEndpoint}`,
    };
    if (isGcp) {
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
        console.log('Using credentialsProfile:', credentialsProfile);
        const credentials = getAwsCredentials(credentialsProfile, '/.aws/credentials');
        params.credentials = credentials;
        
        if (isGcp) {
            console.log('Returning GCP nested structure with credentialsProfile');
            return {
                s3Params: params,
                bucketName,
                mpuBucket: mpuBucketName || bucketName,
                credentials,  // Credential provider for raw HTTP (will be resolved in makeGcpRequest)
            };
        }
        return params;
    }
    
    console.log('Using locCredentials:', locCredentials);
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
