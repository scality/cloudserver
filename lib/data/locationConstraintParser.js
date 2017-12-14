const http = require('http');
const https = require('https');
const AWS = require('aws-sdk');
const Sproxy = require('sproxydclient');

const DataFileBackend = require('./file/backend');
const inMemory = require('./in_memory/backend').backend;
const AwsClient = require('./external/AwsClient');
const GcpClient = require('./external/GcpClient');
const AzureClient = require('./external/AzureClient');
const B2Client = require('./external/B2Client');

const { config } = require('../Config');

const proxyAddress = 'http://localhost:3128';

function parseLC() {
    const clients = {};

    Object.keys(config.locationConstraints).forEach(location => {
        const locationObj = config.locationConstraints[location];
        if (locationObj.type === 'mem') {
            clients[location] = inMemory;
        }
        if (locationObj.type === 'file') {
            clients[location] = new DataFileBackend();
        }
        if (locationObj.type === 'scality'
        && locationObj.details.connector.sproxyd) {
            clients[location] = new Sproxy({
                bootstrap: locationObj.details.connector
                    .sproxyd.bootstrap,
                // Might be undefined which is ok since there is a default
                // set in sproxydclient if chordCos is undefined
                chordCos: locationObj.details.connector.sproxyd.chordCos,
                // Might also be undefined, but there is a default path set
                // in sproxydclient as well
                path: locationObj.details.connector.sproxyd.path,
                // enable immutable optim for all objects
                immutable: true,
            });
            clients[location].clientType = 'scality';
        }
        if (locationObj.type === 'aws_s3' || locationObj.type === 'gcp') {
            if (process.env.CI_PROXY === 'true') {
                locationObj.details.https = false;
                locationObj.details.proxy = proxyAddress;
            }
            const connectionAgent = locationObj.details.https ?
                new https.Agent({ keepAlive: true }) :
                new http.Agent({ keepAlive: true });
            const protocol = locationObj.details.https ? 'https' : 'http';
            const httpOptions = locationObj.details.proxy ?
                { proxy: locationObj.details.proxy, agent: connectionAgent,
                    timeout: 0 }
                : { agent: connectionAgent, timeout: 0 };
            const sslEnabled = locationObj.details.https === true;
            // TODO: HTTP requests to AWS are not supported with V4 auth for
            // non-file streams which are used by Backbeat. This option will be
            // removed once CA certs, proxy setup feature is implemented.
            const signatureVersion = !sslEnabled ? 'v2' : 'v4';
            const endpoint = locationObj.type === 'gcp' ?
                locationObj.details.gcpEndpoint :
                locationObj.details.awsEndpoint;
            const s3Params = {
                endpoint: `${protocol}://${endpoint}`,
                debug: false,
                // Not implemented yet for streams in node sdk,
                // and has no negative impact if stream, so let's
                // leave it in for future use
                computeChecksums: true,
                httpOptions,
                // needed for encryption
                signatureVersion,
                sslEnabled,
                maxRetries: 0,
            };
            // users can either include the desired profile name from their
            // ~/.aws/credentials file or include the accessKeyId and
            // secretAccessKey directly in the locationConfig
            if (locationObj.details.credentialsProfile) {
                s3Params.credentials = new AWS.SharedIniFileCredentials({
                    profile: locationObj.details.credentialsProfile });
            } else {
                s3Params.accessKeyId =
                    locationObj.details.credentials.accessKey;
                s3Params.secretAccessKey =
                    locationObj.details.credentials.secretKey;
            }
            const clientConfig = {
                s3Params,
                bucketName: locationObj.details.bucketName,
                bucketMatch: locationObj.details.bucketMatch,
                serverSideEncryption: locationObj.details.serverSideEncryption,
                dataStoreName: location,
            };
            if (locationObj.type === 'gcp') {
                clientConfig.overflowBucket =
                    locationObj.details.overflowBucketName;
                clientConfig.mpuBucket = locationObj.details.mpuBucketName;
                clientConfig.authParams = config.getGcpServiceParams(location);
            }
            clients[location] = locationObj.type === 'gcp' ?
                new GcpClient(clientConfig) : new AwsClient(clientConfig);
        }
        if (locationObj.type === 'azure') {
            if (process.env.CI_PROXY === 'true') {
                locationObj.details.proxy = proxyAddress;
            }
            const azureStorageEndpoint = config.getAzureEndpoint(location);
            const azureStorageCredentials =
                config.getAzureStorageCredentials(location);
            clients[location] = new AzureClient({
                azureStorageEndpoint,
                azureStorageCredentials,
                azureContainerName: locationObj.details.azureContainerName,
                bucketMatch: locationObj.details.bucketMatch,
                dataStoreName: location,
                proxy: locationObj.details.proxy,
            });
            clients[location].clientType = 'azure';
        }
        if (locationObj.type === 'b2') {
            const b2StorageEndpoint = config.getB2Endpoint(location);
            const b2StorageCredentials =
                config.getB2StorageCredentials(location);
            clients[location] = new B2Client({
                b2StorageEndpoint,
                b2StorageCredentials,
                b2BucketName: locationObj.details.b2BucketName,
                bucketMatch: locationObj.details.bucketMatch,
                dataStoreName: location,
            });
            clients[location].clientType = 'b2';
        }
    });
    return clients;
}

module.exports = parseLC;
