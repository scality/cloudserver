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
            });
            clients[location].clientType = 'scality';
        }
        if (locationObj.type === 'aws_s3') {
            // TODO
        }
    });
    return clients;
}
