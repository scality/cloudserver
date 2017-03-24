import { errors } from 'arsenal';
import { Logger } from 'werelogs';
import async from 'async';

import config from '../Config';
import parseLC from './locationConstraintParser';

const logger = new Logger('MultipleBackendGateway', {
    logLevel: config.log.logLevel,
    dumpLevel: config.log.dumpLevel,
});

function createLogger(reqUids) {
    return reqUids ?
        logger.newRequestLoggerFromSerializedUids(reqUids) :
        logger.newRequestLogger();
}

const clients = parseLC(config);

const multipleBackendGateway = {
    put: (stream, size, keyContext, backendInfo, reqUids, callback) => {
        const controllingLocationConstraint =
            backendInfo.getControllingLocationConstraint();
        const client = clients[controllingLocationConstraint];
        if (!client) {
            const log = createLogger(reqUids);
            log.error('no data backend matching controlling locationConstraint',
            { controllingLocationConstraint });
            return process.nextTick(() => {
                callback(errors.InternalError);
            });
        }
        return client.put(stream, size, keyContext,
            reqUids, (err, key) => {
                if (err) {
                    const log = createLogger(reqUids);
                    log.error('error from datastore',
                             { error: err, implName: client.clientType });
                    return callback(errors.InternalError);
                }
                const dataRetrievalInfo = {
                    key,
                    dataStoreName: controllingLocationConstraint,
                };
                return callback(null, dataRetrievalInfo);
            });
    },

    get: (objectGetInfo, range, reqUids, callback) => {
        const client = clients[objectGetInfo.dataStoreName];
        if (client.clientType === 'scality') {
            return client.get(objectGetInfo.key, range, reqUids, callback);
        }
        return client.get(objectGetInfo, range, reqUids, callback);
    },

    delete: (objectGetInfo, reqUids, callback) => {
        const client = clients[objectGetInfo.dataStoreName];
        if (client.clientType === 'scality') {
            return client.delete(objectGetInfo.key, reqUids, callback);
        }
        return client.delete(objectGetInfo, reqUids, callback);
    },

    healthcheck: (log, callback) => {
        const multBackendResp = {};
        async.each(Object.keys(clients), (location, cb) => {
            const client = clients[location];
            if (client.clientType === 'scality') {
                client.healthcheck(log, (err, res) => {
                    if (err) {
                        multBackendResp[location] = { error: err };
                    } else {
                        multBackendResp[location] = { code: res.statusCode,
                            message: res.statusMessage };
                    }
                    cb();
                });
            } else {
                // if backend isn't 'scality', it will be 'mem' or 'file',
                // for which the default response is 200 OK
                multBackendResp[location] = { code: 200, message: 'OK' };
                cb();
            }
        }, () => {
            callback(null, multBackendResp);
        });
    },
};

export default multipleBackendGateway;
