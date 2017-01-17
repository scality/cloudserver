import AWS from 'aws-sdk';
import Sproxy from 'sproxydclient';

import config from '../Config';

// legacy - no locationConstraints
let legacySproxyd;
if (config.sproxyd) {
    legacySproxyd = new Sproxy({
        bootstrap: config.sproxyd.bootstrap,
        log: config.log,
        chordCos: config.sproxyd.chordCos,
    });
}

const clients = {};
if (config.locationConstraints) {
    Object.keys(config.locationConstraints).forEach(location => {
        const locationObj = config[location];
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
}

// ADD SIMILAR GATEWAY FOR FILE TO INSTANTIATE AWS CLIENTS OR PUT THIS GATEWAY IN FRONT OF BOTH?
// ANY NEED FOR MEM?

// DO WRAPPER STUFF BASED ON REQUEST/STORED INFO
// CLIENT SET IN FOLLOWING PRIORITY ORDER:
// 1) object metadata for object gets
// 2) bucket metadata
// 3) default for endpoint hit for puts
