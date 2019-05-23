const { findCorsRule, generateCorsResHeaders } =
    require('../api/apiUtils/object/corsResponse.js');

/**
 * collectCorsHeaders - gather any relevant CORS headers
 * @param {object} origin - value of Origin header of CORS request
 * @param {object} httpMethod - http method of CORS request
 * @param {BucketInfo} bucket - instance of BucketInfo class
 * @return {object} - object containing CORS headers
 */
function collectCorsHeaders(origin, httpMethod, bucket) {
    // NOTE: Because collecting CORS headers requires making a call to
    // metadata to retrieve the bucket's CORS configuration, we opt not to
    // return the CORS headers if the request encounters an error before
    // the api method retrieves the bucket from metadata (an example
    // being if a request is not properly authenticated). This is a slight
    // deviation from AWS compatibility, but has the benefit of avoiding
    // additional backend calls for an invalid request. Also, we anticipate
    // that the preflight OPTIONS route will serve most client needs regarding
    // CORS.
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
