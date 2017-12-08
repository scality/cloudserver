const { errors } = require('arsenal');

const metadata = require('../metadata/wrapper');
const bucketShield = require('./apiUtils/bucket/bucketShield');
const { findCorsRule, generateCorsResHeaders }
    = require('./apiUtils/object/corsResponse');
// const { pushMetric } = require('../utapi/utilities');

const requestType = 'objectGet';

const customizedErrs = {
    corsNotEnabled: 'CORSResponse: CORS is not enabled for this bucket.',
    notAllowed: 'CORSResponse: This CORS request is not allowed. ' +
    'This is usually because the evalution of Origin, request method / ' +
    'Access-Control-Request-Method or Access-Control-Request-Headers ' +
    'are not whitelisted by the resource\'s CORS spec.',
};

/** corsPreflight - handle preflight CORS requests
* @param  {object} request - http request object
* @param  {function} log - Werelogs request logger
* @param  {function} callback - callback to respond to http request
*  with either error code or 200 response
* @return {undefined}
*/
function corsPreflight(request, log, callback) {
    log.debug('processing request', { method: 'corsPreflight' });

    const bucketName = request.bucketName;
    const corsOrigin = request.headers.origin;
    const corsMethod = request.headers['access-control-request-method'];
    const corsHeaders = request.headers['access-control-request-headers'] ?
        request.headers['access-control-request-headers'].replace(/ /g, '')
            .split(',').reduce((resultArr, value) => {
                // remove empty values and convert values to lowercase
                if (value !== '') {
                    resultArr.push(value.toLowerCase());
                }
                return resultArr;
            }, []) : null;

    return metadata.getBucket(bucketName, log, (err, bucket) => {
        if (err) {
            log.debug('metadata getbucket failed', { error: err });
            return callback(err);
        }
        if (bucketShield(bucket, requestType)) {
            return callback(errors.NoSuchBucket);
        }
        log.trace('found bucket in metadata');

        const corsRules = bucket.getCors();
        if (!corsRules) {
            const err = errors.AccessForbidden
                .customizeDescription(customizedErrs.corsNotEnabled);
            log.trace('no existing cors configuration', {
                error: err,
                method: 'corsPreflight',
            });
            return callback(err);
        }

        log.trace('finding cors rule');
        const corsRule = findCorsRule(corsRules, corsOrigin, corsMethod,
            corsHeaders);

        if (!corsRule) {
            const err = errors.AccessForbidden
                .customizeDescription(customizedErrs.notAllowed);
            log.trace('no matching cors rule', {
                error: err,
                method: 'corsPreflight',
            });
            return callback(err);
        }

        const resHeaders = generateCorsResHeaders(corsRule, corsOrigin,
            corsMethod, corsHeaders, true);
        // TODO: add some level of metrics for non-standard API request:
        // pushMetric('corsPreflight', log, { bucket: bucketName });
        return callback(null, resHeaders);
    });
}

module.exports = corsPreflight;
