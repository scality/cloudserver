import { errors } from 'arsenal';
import { waterfall } from 'async';
import { parseString } from 'xml2js';

import collectCorsHeaders from '../utilities/collectCorsHeaders';
import metadata from '../metadata/wrapper';
import services from '../services';

/**
 * Format of xml request:

 <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Status>VersioningState</Status>
    <MfaDelete>MfaDeleteState</MfaDelete>
 </VersioningConfiguration>

 Note that there is the header in the request if setting MfaDelete:
    x-amz-mfa: [SerialNumber] [TokenCode]
 */

function _parseXML(request, log, cb) {
    if (request.post === '') {
        log.debug('request xml is missing');
        return cb(errors.MalformedXML);
    }
    return parseString(request.post, (err, result) => {
        if (err) {
            log.debug('request xml is malformed');
            return cb(errors.MalformedXML);
        }
        const status = result.VersioningConfiguration.Status ?
            result.VersioningConfiguration.Status[0] : undefined;
        const mfaDelete = result.VersioningConfiguration.MfaDelete ?
            result.VersioningConfiguration.MfaDelete[0] : undefined;
        const validStatuses = ['Enabled', 'Suspended'];
        const validMfaDeletes = [undefined, 'Enabled', 'Disabled'];
        if (validStatuses.indexOf(status) < 0 ||
            validMfaDeletes.indexOf(mfaDelete) < 0) {
            log.debug('illegal versioning configuration');
            return cb(errors.IllegalVersioningConfigurationException);
        }
        if (mfaDelete) {
            log.debug('mfa deletion is not implemented');
            return cb(errors.NotImplemented
                .customizeDescription('MFA Deletion is not supported yet.'));
        }
        return process.nextTick(() => cb(null));
    });
}

/**
 * Bucket Put Versioning - Create or update bucket Versioning
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
export default function bucketPutVersioning(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutVersioning' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketOwnerAction',
        log,
    };

    return waterfall([
        next => _parseXML(request, log, next),
        next => services.metadataValidateAuthorization(metadataValParams,
            (err, bucket) => next(err, bucket)), // ignore extra null object,
        (bucket, next) => parseString(request.post, (err, result) => {
            // just for linting; there should not be any parsing error here
            if (err) {
                return next(err, bucket);
            }
            const versioningConfiguration = {};
            if (result.VersioningConfiguration.Status) {
                versioningConfiguration.Status =
                    result.VersioningConfiguration.Status[0];
            }
            if (result.VersioningConfiguration.MfaDelete) {
                versioningConfiguration.MfaDelete =
                    result.VersioningConfiguration.MfaDelete[0];
            }
            // the configuration has been checked before
            return next(null, bucket, versioningConfiguration);
        }),
        (bucket, versioningConfiguration, next) => {
            bucket.setVersioningConfiguration(versioningConfiguration);
            // TODO all metadata updates of bucket should be using CAS
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutVersioning' });
        }
        // TODO push metrics for bucketPutVersioning
        // else {
        //  pushMetric('bucketPutVersioning', log, {
        //      authInfo,
        //      bucket: bucketName,
        //   }
        // }
        return callback(err, corsHeaders);
    });
}
