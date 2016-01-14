import authV2 from './v2/authV2.js';

function auth(request, log, cb) {
    const authHeader = request.lowerCaseHeaders.authorization;

    // Check whether signature is in header
    if (authHeader) {
        log.debug(`Authorization header: ${authHeader}`);
        // TODO: Check for security token header to
        // handle temporary security credentials
        if (authHeader.startsWith('AWS ')) {
            log.debug('Authenticating request with Auth V2 using headers');
            authV2.headerAuthCheck(request, log, cb);
        } else if (authHeader.startsWith('AWS4')) {
            // TODO: Deal with v4HeaderAuth
            log.debug('Authenticating request with Auth V4 using headers');
            log.debug('Authorization V4: Not Implemented');
            return cb('NotImplemented');
        } else {
            log.error('Unable to authenticate request: ' +
                'Missing Authorization Security Header');
            return cb('MissingSecurityHeader');
        }
    } else if (request.query.Signature) {
        // Check whether signature is in query string
        log.debug('Authenticating request with Auth V2 using query string');
        authV2.queryAuthCheck(request, log, cb);
    } else if (request.query['X-Amz-Algorithm']) {
        // TODO: Handle v4 query scenario
        log.debug('Authorization V4: Not Implemented');
        return cb('NotImplemented');
    } else {
        // If no auth information is provided in request, then
        // user is part of 'All Users Group' so send back this
        // group as the accessKey. This means the user will only
        // be able to perform actions that are public.
        log.debug('No authentication provided. User identified as public');
        return cb(null, 'http://acs.amazonaws.com/groups/global/AllUsers');
    }
}

export default auth;
