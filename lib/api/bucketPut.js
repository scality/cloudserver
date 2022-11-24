const { waterfall } = require('async');
const { parseString } = require('xml2js');
const { auth, errors, policies } = require('arsenal');

const vault = require('../auth/vault');
const { createBucket } = require('./apiUtils/bucket/bucketCreation');
const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const { config } = require('../Config');
const aclUtils = require('../utilities/aclUtils');
const { pushMetric } = require('../utapi/utilities');
const requestUtils = policies.requestUtils;

let { restEndpoints, locationConstraints } = config;
config.on('rest-endpoints-update', () => {
    restEndpoints = config.restEndpoints;
});

config.on('location-constraints-update', () => {
    locationConstraints = config.locationConstraints;
});

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
        const errMsg = 'value of the location you are attempting to set - ' +
        `${locationConstraintChecked} - is not listed in the ` +
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

function _buildConstantParams({ request, bucketName, authInfo, authParams, ip, locationConstraint, apiMethod }) {
    return {
        constantParams: {
            headers: request.headers,
            query: request.query,
            generalResource: bucketName,
            specificResource: {
                key: '',
            },
            requesterIp: ip,
            sslEnabled: request.connection.encrypted,
            awsService: 's3',
            requesterInfo: authInfo,
            signatureVersion: authParams.params.data.authType,
            authType: authParams.params.data.signatureVersion,
            signatureAge: authParams.params.data.signatureAge,
            apiMethod,
            locationConstraint,
        },
    };
}

function _handleAuthResults(locationConstraint, log, cb) {
    return (err, authorizationResults) => {
        if (err) {
            return cb(err);
        }
        if (!authorizationResults.every(res => {
            if (Array.isArray(res)) {
                return res.every(subRes => subRes.isAllowed);
            }
            return res.isAllowed;
        })) {
            log.trace(
                'authorization check failed for user',
                { locationConstraint },
            );
            return cb(errors.AccessDenied);
        }
        return cb(null, locationConstraint);
    };
}

function _isObjectLockEnabled(headers) {
    const header = headers['x-amz-bucket-object-lock-enabled'];
    return header !== undefined && header.toLowerCase() === 'true';
}

function authBucketPut(authParams, bucketName, locationConstraint, request, authInfo) {
    const ip = requestUtils.getClientIp(request, config);
    const baseParams = {
        authParams,
        ip,
        bucketName,
        request,
        authInfo,
        locationConstraint,
    };
    const requestConstantParams = [Object.assign(
        baseParams,
        { apiMethod: 'bucketPut' },
    )];

    if (_isObjectLockEnabled(request.headers)) {
        requestConstantParams.push(Object.assign(
            {},
            baseParams,
            { apiMethod: 'bucketPutObjectLock' },
        ));
        requestConstantParams.push(Object.assign(
            {},
            baseParams,
            { apiMethod: 'bucketPutVersioning' },
        ));
    }
    return requestConstantParams;
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

    if (request.bucketName === 'METADATA') {
        return callback(errors.AccessDenied
            .customizeDescription('The bucket METADATA is used ' +
            'for internal purposes'));
    }

    return waterfall([
        next => _parseXML(request, log, next),
        (locationConstraint, next) => {
            // Check policies in Vault for a user.
            if (!authInfo.isRequesterAnIAMUser()) {
                return next(null, locationConstraint);
            }

            const authParams = auth.server.extractParams(request, log, 's3', request.query);
            const requestConstantParams = authBucketPut(
                authParams, bucketName, locationConstraint, request, authInfo
            );

            return vault.checkPolicies(
                requestConstantParams.map(_buildConstantParams),
                authInfo.getArn(),
                log,
                _handleAuthResults(locationConstraint, log, next),
            );
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
    _handleAuthResults,
    authBucketPut,
};
