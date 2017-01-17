import { errors } from 'arsenal';
import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';
import routeOPTIONS from './routes/routeOPTIONS';
import routesUtils from './routes/routesUtils';
import routeWebsite from './routes/routeWebsite';
import utils from './utils';
import { healthcheckHandler } from './utilities/healthcheckHandler';
import _config from './Config';
import RedisClient from './RedisClient';
import StatsClient from './StatsClient';

const routeMap = {
    GET: routeGET,
    PUT: routePUT,
    POST: routePOST,
    DELETE: routeDELETE,
    HEAD: routeHEAD,
    OPTIONS: routeOPTIONS,
};

// redis client
let localCacheClient;
if (_config.localCache) {
    localCacheClient = new RedisClient(_config.localCache.host,
        _config.localCache.port);
}
// stats client
const STATS_INTERVAL = 5; // 5 seconds
const STATS_EXPIRY = 30; // 30 seconds
const statsClient = new StatsClient(localCacheClient, STATS_INTERVAL,
    STATS_EXPIRY);

function checkUnsuportedRoutes(req, res, log) {
    if (req.query.policy !== undefined ||
        req.query.tagging !== undefined) {
        return routesUtils.responseXMLBody(
            errors.NotImplemented, null, res, log);
    }
    const method = routeMap[req.method.toUpperCase()];
    if (method) {
        return method(req, res, log, statsClient);
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
        return healthcheckHandler(clientInfo.clientIP, false, req, res, log,
            statsClient);
    } else if (req.url === '/_/healthcheck/deep') {
        return healthcheckHandler(clientInfo.clientIP, true, req, res, log);
    }
    // report new request for stats
    statsClient.reportNewRequest();

    try {
        utils.normalizeRequest(req);
    } catch (err) {
        log.trace('could not normalize request', { error: err.stack });
        return routesUtils.responseXMLBody(
            errors.InvalidURI, undefined, res, log);
    }

    log.addDefaultFields({
        bucketName: req.bucketName,
        objectKey: req.objectKey,
        bytesReceived: req.parsedContentLength || 0,
        bodyLength: parseInt(req.headers['content-length'], 10) || 0,
    });
    // if empty name and request not a list Buckets
    if (!req.bucketName &&
      !(req.method.toUpperCase() === 'GET' && !req.objectKey)) {
        log.warn('empty bucket name', { method: 'routes' });
        const err = (req.method.toUpperCase() !== 'OPTIONS') ?
        errors.MethodNotAllowed : errors.AccessForbidden
                .customizeDescription('CORSResponse: Bucket not found');
        return routesUtils.responseXMLBody(err, undefined, res, log);
    }

    if (req.bucketName !== undefined &&
        utils.isValidBucketName(req.bucketName) === false) {
        log.warn('invalid bucket name', { bucketName: req.bucketName });
        return routesUtils.responseXMLBody(errors.InvalidBucketName,
            undefined, res, log);
    }

    // bucket website request
    if (_config.websiteEndpoints.indexOf(req.parsedHost) > -1) {
        return routeWebsite(req, res, log, statsClient);
    }

    return checkUnsuportedRoutes(req, res, log);
}
