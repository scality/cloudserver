const { findCorsRule, generateCorsResHeaders } = require('../api/apiUtils/object/corsResponse.js');

/**
 * collectCorsHeaders - gather any relevant CORS headers
 * @param {object} origin - value of Origin header of CORS request
 * @param {object} httpMethod - http method of CORS request
 * @param {BucketInfo} bucket - instance of BucketInfo class
 * @return {object} - object containing CORS headers
 */
function collectCorsHeaders(origin, httpMethod, bucket) {
    // Returns {} when no bucket is supplied; this function does not itself
    // fetch metadata. Callers that only have a bucketName (e.g. auth
    // failures that happen before the API handler loads the bucket) should
    // look it up themselves - see wrapCallbackWithErrorCorsHeaders in
    // lib/api/api.js.
    if (!origin || !bucket) {
        return {};
    }
    const corsRules = bucket.getCors();
    if (!corsRules) {
        return {};
    }
    const matchingRule = findCorsRule(corsRules, origin, httpMethod, null);
    if (!matchingRule) {
        return {};
    }
    return generateCorsResHeaders(matchingRule, origin, httpMethod, null);
}

module.exports = collectCorsHeaders;
