import { waterfall } from 'async';
import { parseString } from 'xml2js';

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
        next => services.metadataValidateAuthorization(metadataValParams,
            (err, bucket) => next(err, bucket)), // to ignore null object
        (bucket, next) => parseString(request.post, (err, result) => {
            // just for linting; there should not be any parsing error here
            if (err) {
                return next(err);
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
            metadata.updateBucket(bucket.getName(), bucket, log, next);
        },
    ], err => {
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutVersioning' });
            return callback(err);
        }
        // TODO push metrics for bucketPutVersioning
        // pushMetric('bucketPutVersioning', log, {
        //      bucket: bucketName,
        // }
        return callback(err, 'Versioning Set');
    });
}
