import { errors } from 'arsenal';

import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';
import routeUtils from './routes/routesUtils';
import utils from './utils';

export default function routes(req, res, logger) {
    const clientIP = req.headers['x-forwarded-for'] ||
        req.connection.remoteAddress || req.socket.remoteAddress;

    const log = logger.newRequestLogger();
    log.addDefaultFields({
        clientIP,
        url: req.url,
        httpMethod: req.method,
    });
    log.debug('routing request');

    try {
        utils.normalizeRequest(req);
    } catch (err) {
        log.warn('could not normalize request', { error: err });
        return routeUtils.responseXMLBody(
            errors.InvalidURI, undefined, res, log,
            routeUtils.onRequestEnd(req));
    }

    log.addDefaultFields({
        bucketName: req.bucketName,
        objectKey: req.objectKey,
    });

    if (req.bucketName !== undefined &&
        utils.isValidBucketName(req.bucketName) === false) {
        log.warn('invalid bucket name', { bucketName: req.bucketName });
        return routeUtils.responseXMLBody(errors.InvalidBucketName,
            undefined, res, log, routeUtils.onRequestEnd(req));
    }

    switch (req.method) {
    case 'GET':
        return routeGET(req, res, log);
    case 'PUT':
        return routePUT(req, res, log);
    case 'POST':
        return routePOST(req, res, log);
    case 'DELETE':
        return routeDELETE(req, res, log);
    case 'HEAD':
        return routeHEAD(req, res, log);
    default:
        routeUtils.responseXMLBody(errors.MethodNotAllowed, null, res, log,
                                   routeUtils.onRequestEnd(req));
    }
}
