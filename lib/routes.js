import { errors, ipCheck } from 'arsenal';
import { UtapiClient } from 'utapi';

import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';
import routesUtils from './routes/routesUtils';
import utils from './utils';
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

// current function utility is minimal, but will be expanded
export function isHealthy() {
    return true;
}

function writeResponse(res, error, log) {
    const statusCode = error ? error.code : 200;
    const body = error ? JSON.stringify(error) : '{}';

    res.writeHead(statusCode, { 'Content-Type': 'application/json' });
    res.write(body);

    res.end(() => {
        log.end().info('healthcheck ended', {
            httpCode: res.statusCode,
        });
    });
}

function healthcheckRouteHandler(req, res, log) {
    if (isHealthy()) {
        if (req.method === 'GET' || req.method === 'POST') {
            writeResponse(res, null, log);
        } else {
            writeResponse(res, errors.BadRequest, log);
        }
    } else {
        writeResponse(res, errors.InternalError, log);
    }
    return res.statusCode;
}

function checkIP(clientIP) {
    return ipCheck.ipMatchCidrList(
        _config.healthChecks.allowFrom, clientIP);
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
        if (!checkIP(clientInfo.clientIP)) {
            return writeResponse(res, errors.AccessDenied, log);
        }
        return healthcheckRouteHandler(req, res, log);
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

    if (req.bucketName !== undefined &&
        utils.isValidBucketName(req.bucketName) === false) {
        log.warn('invalid bucket name', { bucketName: req.bucketName });
        return routesUtils.responseXMLBody(errors.InvalidBucketName,
            undefined, res, log);
    }

    return checkUnsuportedRoutes(req, res, log);
}
