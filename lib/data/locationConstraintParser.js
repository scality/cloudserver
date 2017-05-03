import AWS from 'aws-sdk';
import file from './file/backend';
import inMemory from './in_memory/backend';
import Sproxy from 'sproxydclient';

import config from '../Config';

export default function parseLC() {
    const clients = {};

    Object.keys(config.locationConstraints).forEach(location => {
        const locationObj = config.locationConstraints[location];
        if (locationObj.type === 'mem') {
            clients[location] = inMemory;
        }
        if (locationObj.type === 'file') {
            clients[location] = file;
        }
        if (locationObj.type === 'scality'
        && locationObj.details.connector.sproxyd) {
            clients[location] = new Sproxy({
                bootstrap: locationObj.details.connector
                    .sproxyd.bootstrap,
                log: config.log,
                // Might be undefined which is ok since there is a default
                // set in sproxydclient if chordCos is undefined
                chordCos: locationObj.details.connector.sproxyd.chordCos,
                // Might also be undefined, but there is a default path set
                // in sproxydclient as well
                path: locationObj.details.connector.sproxyd.path,
            });
            clients[location].clientType = 'scality';
        }
        if (locationObj.type === 'aws_s3') {
            // users can either include the desired profile name from their
            // ~/.aws/credentials file or include the accessKeyId and
            // secretAccessKey directly in the locationConfig
            if (locationObj.details.credentialsProfile) {
                clients[location] = new AWS.S3({
                    endpoint: `https://${locationObj.details.awsEndpoint}`,
                    // Non-file stream objects are not supported
                    // with SigV4 (node sdk)
                    // so note that we are using the default of
                    // signatureVersion v2

                    // consider disabling
                    debug: true,
                    // perhaps use this rather than setting ourselves.
                    // Not implemented yet for streams in node sdk!!!
                    computeChecksums: true,
                    credentials: new AWS.SharedIniFileCredentials({ profile:
                        locationObj.details.credentialsProfile }),
                });
            } else {
                clients[location] = new AWS.S3({
                    endpoint: `https://${locationObj.details.awsEndpoint}`,
                    debug: true,
                    computeChecksums: true,
                    accessKeyId: locationObj.details.credentials.accessKey,
                    secretAccessKey: locationObj.details.credentials.secretKey,
                });
            }
            clients[location].clientType = 'aws_s3';
            clients[location].awsBucketName = locationObj.details.bucketName;
            clients[location].bucketMatch = locationObj.details.bucketMatch;
            clients[location].dataStoreName = location;
        }
    });
    return clients;
}
