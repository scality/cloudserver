const { waterfall } = require('async');
const { parseString } = require('xml2js');
const { auth, errors } = require('arsenal');

const vault = require('../auth/vault');
const { createBucket } = require('./apiUtils/bucket/bucketCreation');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { config } = require('../Config');
const aclUtils = require('../utilities/aclUtils');
const { pushMetric } = require('../utapi/utilities');

const { locationConstraints, restEndpoints } = config;


/**
 * checkLocationConstraint - check that a location constraint is explicitly
 * set on the bucket and the value of the location is listed in the
 * locationConstraint config.
 * Note: if data backend equals "multiple", you must set a location constraint
 * @param {object} request - http request object
 * @param {string} locationConstraint - the location constraint sent with
 * the xml of the request
 * @param {object} log - Werelogs logger
 * @return {undefined}
 */
function checkLocationConstraint(request, locationConstraint, log) {
    // AWS JS SDK sends a request with locationConstraint us-east-1 if
    // no locationConstraint provided.
    const { parsedHost } = request;
    let locationConstraintChecked;
    if (locationConstraint) {
        locationConstraintChecked = locationConstraint;
    } else if (parsedHost && restEndpoints[parsedHost]) {
        locationConstraintChecked = restEndpoints[parsedHost];
    } else {
        log.trace('no location constraint provided on bucket put;' +
            'setting us-east-1');
        locationConstraintChecked = 'us-east-1';
    }

    if (!locationConstraints[locationConstraintChecked]) {
        const errMsg = 'value of the location is not listed in the ' +
        'locationConstraint config';
        log.trace(`locationConstraint is invalid - ${errMsg}`,
          { locationConstraint: locationConstraintChecked });
        return { error: errors.InvalidLocationConstraint.
          customizeDescription(errMsg) };
    }
    return { error: null, locationConstraint: locationConstraintChecked };
}

/*
   Format of xml request:

   <?xml version="1.0" encoding="UTF-8"?>
   <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <LocationConstraint>us-west-1</LocationConstraint>
   </CreateBucketConfiguration>
   */

function _parseXML(request, log, cb) {
    if (request.post) {
        return parseString(request.post, (err, result) => {
            if (err || !result.CreateBucketConfiguration
                || !result.CreateBucketConfiguration.LocationConstraint
                || !result.CreateBucketConfiguration.LocationConstraint[0]) {
                log.debug('request xml is malformed');
                return cb(errors.MalformedXML);
            }
            const locationConstraint = result.CreateBucketConfiguration
                .LocationConstraint[0];
            log.trace('location constraint',
                { locationConstraint });
            const locationCheck = checkLocationConstraint(request,
              locationConstraint, log);
            if (locationCheck.error) {
                return cb(locationCheck.error);
            }
            return cb(null, locationCheck.locationConstraint);
        });
    }
    return process.nextTick(() => {
        const locationCheck = checkLocationConstraint(request,
          undefined, log);
        if (locationCheck.error) {
            return cb(locationCheck.error);
        }
        return cb(null, locationCheck.locationConstraint);
    });
}

/**
 * PUT Service - Create bucket for the user
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPut(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPut' });

    if (authInfo.isRequesterPublicUser()) {
        log.debug('operation not available for public user');
        return callback(errors.AccessDenied);
    }
    if (!aclUtils.checkGrantHeaderValidity(request.headers)) {
        log.trace('invalid acl header');
        return callback(errors.InvalidArgument);
    }
    const { bucketName } = request;

    return waterfall([
        next => _parseXML(request, log, next),
        // Check policies in Vault for a user.
        (locationConstraint, next) => {
            if (authInfo.isRequesterAnIAMUser()) {
                const authParams = auth.server.extractParams(request, log, 's3',
                    request.query);
                const requestContextParams = {
                    constantParams: {
                        headers: request.headers,
                        query: request.query,
                        generalResource: bucketName,
                        specificResource: {
                            key: '',
                        },
                        requesterIp: request.socket.remoteAddress,
                        sslEnabled: request.connection.encrypted,
                        apiMethod: 'bucketPut',
                        awsService: 's3',
                        locationConstraint,
                        requesterInfo: authInfo,
                        signatureVersion: authParams.params.data.authType,
                        authType: authParams.params.data.signatureVersion,
                        signatureAge: authParams.params.data.signatureAge,
                    },
                };
                return vault.checkPolicies(requestContextParams,
                    authInfo.getArn(), log, (err, authorizationResults) => {
                        if (err) {
                            return next(err);
                        }
                        if (authorizationResults[0].isAllowed !== true) {
                            log.trace('authorization check failed for user',
                                { locationConstraint });
                            return next(errors.AccessDenied);
                        }
                        return next(null, locationConstraint);
                    });
            }
            return next(null, locationConstraint);
        },
        (locationConstraint, next) => createBucket(authInfo, bucketName,
          request.headers, locationConstraint, log, (err, previousBucket) => {
              // if bucket already existed, gather any relevant cors
              // headers
              const corsHeaders = collectCorsHeaders(
                  request.headers.origin, request.method, previousBucket);
              if (err) {
                  return next(err, corsHeaders);
              }
              pushMetric('createBucket', log, {
                  authInfo,
                  bucket: bucketName,
              });
              return next(null, corsHeaders);
          }),
    ], callback);
}

module.exports = {
    checkLocationConstraint,
    bucketPut,
};
