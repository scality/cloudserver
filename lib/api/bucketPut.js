import { auth, errors } from 'arsenal';
import vault from '../auth/vault';
import { parseString } from 'xml2js';
import { waterfall } from 'async';
import { createBucket } from './apiUtils/bucket/bucketCreation';
import collectCorsHeaders from '../utilities/collectCorsHeaders';
import config from '../Config';
import aclUtils from '../utilities/aclUtils';
import { pushMetric } from '../utapi/utilities';

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
            return cb(null, locationConstraint);
        });
    }
    // We need the second parameter to be `undefined` so the waterfall maintains
    // the position of the `next` argument.
    return process.nextTick(() => cb(null, undefined));
}

/**
 * PUT Service - Create bucket for the user
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPut(authInfo, request, log, callback) {
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
                        specificResource: '',
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
        (locationConstraint, next) => {
            // AWS JS SDK sends a request with locationConstraint us-east-1 if
            // no locationConstraint provided.
            const { locationConstraints, restEndpoints } = config;
            const { parsedHost } = request;
            if (locationConstraint && Object.keys(locationConstraints)
                .indexOf(locationConstraint) < 0) {
                log.trace('locationConstraint is invalid',
                    { locationConstraint });
                return next(errors.InvalidLocationConstraint);
            }
            let locationConstraintChecked;
            if (!locationConstraint && parsedHost &&
                restEndpoints[parsedHost]) {
                locationConstraintChecked = restEndpoints[parsedHost];
            } else {
                locationConstraintChecked = locationConstraint;
            }
            return createBucket(authInfo, bucketName, request.headers,
                locationConstraintChecked, log, (err, previousBucket) => {
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
                });
        },
    ], callback);
}
