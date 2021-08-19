const { waterfall } = require('async');
const { parseString } = require('xml2js');
const { errors } = require('arsenal');

const collectCorsHeaders = require('../utilities/collectCorsHeaders');
const metadata = require('../metadata/wrapper');
const { metadataValidateBucket } = require('../metadata/metadataUtils');
const { pushMetric } = require('../utapi/utilities');
const versioningNotImplBackends =
    require('../../constants').versioningNotImplBackends;
const { config } = require('../Config');
const monitoring = require('../utilities/monitoringHandler');

const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type Azure or GCP.';

const replicationVersioningErrorMessage = 'A replication configuration is ' +
'present on this bucket, so you cannot change the versioning state. To ' +
'change the versioning state, first delete the replication configuration.';

const ingestionVersioningErrorMessage = 'Versioning cannot be suspended for '
+ 'buckets setup with Out of Band updates from a location';

const invalidBucketStateMessage = 'A replication configuration is present on ' +
 'this bucket, so you cannot change the versioning state. To change the ' +
 'versioning state, first delete the replication configuration.';

const objectLockErrorMessage = 'An Object Lock configuration is present on ' +
  'this bucket, so the versioning state cannot be changed.';

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
        const versioningConf = result.VersioningConfiguration;
        const status = versioningConf.Status ?
            versioningConf.Status[0] : undefined;
        const mfaDelete = versioningConf.MfaDelete ?
            versioningConf.MfaDelete[0] : undefined;
        const validStatuses = ['Enabled', 'Suspended'];
        const validMfaDeletes = [undefined, 'Enabled', 'Disabled'];
        if (validStatuses.indexOf(status) < 0 ||
            validMfaDeletes.indexOf(mfaDelete) < 0) {
            log.debug('illegal versioning configuration');
            return cb(errors.IllegalVersioningConfigurationException);
        }
        if (versioningConf && mfaDelete === 'Enabled') {
            log.debug('mfa deletion is not implemented');
            return cb(errors.NotImplemented
                .customizeDescription('MFA Deletion is not supported yet.'));
        }
        return process.nextTick(() => cb(null));
    });
}

function _checkBackendVersioningImplemented(bucket) {
    const bucketLocation = bucket.getLocationConstraint();
    const bucketLocationType = config.getLocationConstraintType(bucketLocation);

    // backend types known not to support versioning
    if (versioningNotImplBackends[bucketLocationType]) {
        return false;
    }

    // versioning disabled per-location constraint
    const lc = config.getLocationConstraint(bucketLocation);
    if (lc.details && !lc.details.supportsVersioning) {
        return false;
    }

    return true;
}

function _isValidVersioningRequest(result, bucket) {
    if (result.VersioningConfiguration &&
        result.VersioningConfiguration.Status) {
        const restricted = bucket.getReplicationConfiguration()
            || (bucket.isIngestionBucket && bucket.isIngestionBucket());
        // Is there a replication configuration set on the bucket or the bucket
        // is an ingestion bucket and is the user attempting to suspend
        // versioning?
        if (restricted) {
            return result.VersioningConfiguration.Status[0] !== 'Suspended';
        }
    }
    return true;
}

/**
 * Bucket Put Versioning - Create or update bucket Versioning
 * @param {AuthInfo} authInfo - Instance of AuthInfo class with requester's info
 * @param {object} request - http request object
 * @param {object} log - Werelogs logger
 * @param {function} callback - callback to server
 * @return {undefined}
 */
function bucketPutVersioning(authInfo, request, log, callback) {
    log.debug('processing request', { method: 'bucketPutVersioning' });

    const bucketName = request.bucketName;
    const metadataValParams = {
        authInfo,
        bucketName,
        requestType: 'bucketPutVersioning',
        request,
    };
    return waterfall([
        next => _parseXML(request, log, next),
        next => metadataValidateBucket(metadataValParams, log,
            (err, bucket) => next(err, bucket)), // ignore extra null object,
        (bucket, next) => parseString(request.post, (err, result) => {
            // just for linting; there should not be any parsing error here
            if (err) {
                return next(err, bucket);
            }
            // prevent enabling versioning on an nfs exported bucket
            if (bucket.isNFS()) {
                const error = new Error();
                error.code = 'NFSBUCKET';
                return next(error);
            }
            // _checkBackendVersioningImplemented returns false if versioning
            // is not implemented on the bucket backend
            if (!_checkBackendVersioningImplemented(bucket)) {
                log.debug(externalVersioningErrorMessage,
                    { method: 'bucketPutVersioning',
                    error: errors.NotImplemented });
                const error = errors.NotImplemented.customizeDescription(
                    externalVersioningErrorMessage);
                return next(error, bucket);
            }
            if (!_isValidVersioningRequest(result, bucket)) {
                const errorMsg =
                (bucket.isIngestionBucket && bucket.isIngestionBucket()) ?
                    ingestionVersioningErrorMessage :
                    replicationVersioningErrorMessage;
                log.debug(errorMsg, {
                    method: 'bucketPutVersioning',
                    error: errors.InvalidBucketState,
                });
                const error = errors.InvalidBucketState
                    .customizeDescription(errorMsg);
                return next(error, bucket);
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
            // check if replication is enabled if versioning is being suspended
            const replicationConfig = bucket.getReplicationConfiguration();
            const invalidAction =
                versioningConfiguration.Status === 'Suspended'
                && replicationConfig
                && replicationConfig.rules
                && replicationConfig.rules.some(r => r.enabled);
            if (invalidAction) {
                next(errors.InvalidBucketState
                    .customizeDescription(invalidBucketStateMessage));
                return;
            }
            const objectLockEnabled = bucket.isObjectLockEnabled();
            if (objectLockEnabled) {
                next(errors.InvalidBucketState
                    .customizeDescription(objectLockErrorMessage));
                return;
            }
            bucket.setVersioningConfiguration(versioningConfiguration);
            // TODO all metadata updates of bucket should be using CAS
            metadata.updateBucket(bucket.getName(), bucket, log, err =>
                next(err, bucket));
        },
    ], (err, bucket) => {
        const corsHeaders = collectCorsHeaders(request.headers.origin,
            request.method, bucket);
        if (err && err.code === 'NFSBUCKET') {
            log.trace('skipping versioning for nfs exported bucket');
            return callback(null, corsHeaders);
        }
        if (err) {
            log.trace('error processing request', { error: err,
                method: 'bucketPutVersioning' });
            monitoring.promMetrics(
                'PUT', bucketName, err.code, 'putBucketVersioning');
        } else {
            pushMetric('putBucketVersioning', log, {
                authInfo,
                bucket: bucketName,
            });
            monitoring.promMetrics(
                'PUT', bucketName, '200', 'putBucketVersioning');
        }
        return callback(err, corsHeaders);
    });
}

module.exports = bucketPutVersioning;
