import routeGET from './routes/routeGET';
import routePUT from './routes/routePUT';
import routeDELETE from './routes/routeDELETE';
import routeHEAD from './routes/routeHEAD';
import routePOST from './routes/routePOST';
import routeUtils from './routes/routesUtils';
import utils from './utils';

export default function routes(req, res, logger) {
    const log = logger.newRequestLogger();
    log.debug(`Routing ${req.method}: ${req.url}`);

    try {
        utils.normalizeRequest(req);
    } catch (err) {
        log.debug(`could not normalize request: ${err}`);
        return routeUtils.responseXMLBody(
            'InvalidURI', undefined, res, log);
    }

    if (req.bucketName !== undefined &&
        utils.isValidBucketName(req.bucketName) === false) {
        log.debug(`Bucket name: ${req.bucketName} is invalid`);
        return routeUtils.responseXMLBody('InvalidBucketName',
            undefined, res, log);
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
        routeUtils.responseXMLBody('MethodNotAllowed', null, res, log);
    }
}
