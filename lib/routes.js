import { errors } from 'arsenal';
import { UtapiClient } from 'utapi';

import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';
import routesUtils from './routes/routesUtils';
import utils from './utils';
import healthcheckHandler from './utilities/healthcheckHandler';
import _config from './Config';

const routeMap = {
    GET: routeGET,
    PUT: routePUT,
    POST: routePOST,
    DELETE: routeDELETE,
    HEAD: routeHEAD,
};

// setup utapi client
const utapi = new UtapiClient(_config.utapi);

function checkUnsuportedRoutes(req, res, log) {
    if (req.query.policy !== undefined ||
        req.query.cors !== undefined ||
        req.query.tagging !== undefined) {
        return routesUtils.responseXMLBody(
            errors.NotImplemented, null, res, log);
    }
    const method = routeMap[req.method.toUpperCase()];
    if (method) {
        return method(req, res, log, utapi);
    }
    return routesUtils.responseXMLBody(errors.MethodNotAllowed, null, res, log);
}

export default function routes(req, res, logger) {
    const clientInfo = {
        clientIP: req.socket.remoteAddress,
        clientPort: req.socket.remotePort,
        httpMethod: req.method,
        httpURL: req.url,
    };

    const log = logger.newRequestLogger();
    log.info('received request', clientInfo);

    log.end().addDefaultFields(clientInfo);

    if (req.url === '/_/healthcheck') {
        return healthcheckHandler(clientInfo.clientIP, false, req, res, log);
    } else if (req.url === '/_/healthcheck/deep') {
        return healthcheckHandler(clientInfo.clientIP, true, req, res, log);
    }

    try {
        utils.normalizeRequest(req);
    } catch (err) {
        log.trace('could not normalize request', { error: err });
        return routesUtils.responseXMLBody(
            errors.InvalidURI, undefined, res, log);
    }

    log.addDefaultFields({
        bucketName: req.bucketName,
        objectKey: req.objectKey,
    });
    // if empty name and request not a list Buckets
    if (!req.bucketName &&
      !(req.method.toUpperCase() === 'GET' && !req.objectKey)) {
        log.warn('empty bucket name', { method: 'routes' });
        return routesUtils.responseXMLBody(errors.MethodNotAllowed,
            undefined, res, log);
    }

    if (req.bucketName !== undefined &&
        utils.isValidBucketName(req.bucketName) === false) {
        log.warn('invalid bucket name', { bucketName: req.bucketName });
        return routesUtils.responseXMLBody(errors.InvalidBucketName,
            undefined, res, log);
    }

    return checkUnsuportedRoutes(req, res, log);
}
