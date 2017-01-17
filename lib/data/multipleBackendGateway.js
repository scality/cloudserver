import AWS from 'aws-sdk';
import Sproxy from 'sproxydclient';

import file from './file/backend';
import inMemory from './in_memory/backend';
import config from '../Config';

const clients = {};
Object.keys(config.locationConstraints).forEach(location => {
    const locationObj = config[location];
    if (locationObj.type === 'mem') {
        clients[location] = inMemory;
    }
    if (locationObj.type === 'file') {
        clients[location] = file;
    }
    if (locationObj.type === 'scality_s3'
    && locationObj.information.connector === 'sproxyd') {
        clients[location] = new Sproxy({
            bootstrap: locationObj.information.connector
                .sproxyd.bootstrap,
            log: config.log,
            // Might be undefined which is ok since there is a default
            // set in sproxydclient if chordCos is undefined
            chordCos: locationObj.information.connector.sproxyd.chordCos,
        });
    }
    if (locationObj.type === 'aws_s3') {
        clients[location] = new AWS.S3({
            endpoint: `https://${locationObj.information.bucketName}` +
                `s3.${locationObj.information.region}.amazonaws.com`,
            signatureVersion: 'v4',
            credentials: new AWS.SharedIniFileCredentials({ profile:
                locationObj.information.credentialsProfile }),
        });
    }
    if (locationObj.type === 'virtual-user-metadata') {
        // TODO
        // clients[location] = some sort of bucketclient
    }
});

const multipleBackendGateway = {
    put: (writeStream, size, keyContext, reqUids, backendInfo, callback) => {

    },

    get: (key, range, reqUids, callback) => {

    },

    delete: (key, reqUids, callback) => {

    }

    // checkHealth: to aggregate from multiple?
};

export default multipleBackendGateway;

// DO WRAPPER STUFF BASED ON REQUEST/STORED INFO
//
// For GETS and DELETES use objectGetInfo implName

// For PUTS:
// 1) check x-amz-meta-scal-location-constraint on put/copy
// 2) bucket location constraint
// 3) default for endpoint hit.
