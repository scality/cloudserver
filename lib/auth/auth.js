import authV2 from './v2/authV2.js';

function auth(request, cb) {
    const authHeader = request.lowerCaseHeaders.authorization;
    // Check whether signature is in header
    if (authHeader) {
        // TODO: Check for security token header to
        // handle temporary security credentials
        if (authHeader.startsWith('AWS ')) {
            authV2.headerAuthCheck(request, cb);
        } else if (authHeader.startsWith('AWS4')) {
            // TODO: Deal with v4HeaderAuth
            return cb('NotImplemented');
        } else {
            return cb('MissingSecurityHeader');
        }
    } else if (request.query.Signature) {
        // Check whether signature is in query string
        authV2.queryAuthCheck(request, cb);
    } else if (request.query['X-Amz-Algorithm']) {
        // TODO: Handle v4 query scenario
        return cb('NotImplemented');
    } else {
        // If no auth information is provided in request, then
        // user is part of 'All Users Group' so send back this
        // group as the accessKey. This means the user will only
        // be able to perform actions that are public.
        return cb(null, 'http://acs.amazonaws.com/groups/global/AllUsers');
    }
}

export default auth;
