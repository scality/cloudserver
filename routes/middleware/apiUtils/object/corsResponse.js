/** _matchesValue - compare two values to determine if they match
* @param {string} allowedValue - an allowed value in a CORS rule;
*    may contain wildcards
* @param {string} value - value from CORS request
* @return {boolean} - true/false
*/
function _matchesValue(allowedValue, value) {
    const wildcardIndex = allowedValue.indexOf('*');
    // If no wildcards, simply return whether strings are equal
    if (wildcardIndex === -1) {
        return allowedValue === value;
    }
    // Otherwise make sure substrings match expected substrings before
    // and after the wildcard
    const beginValue = allowedValue.substring(0, wildcardIndex);
    const endValue = allowedValue.substring(wildcardIndex + 1);
    return (value.startsWith(beginValue) && value.endsWith(endValue));
}

/** _matchesOneOf - check if header matches any AllowedHeaders of a rule
* @param {string[]} allowedHeaders - headers allowed in CORS rule
* @param {string} header - header from CORS request
* @return {boolean} - true/false
*/
function _matchesOneOf(allowedHeaders, header) {
    return allowedHeaders.some(allowedHeader =>
        // AllowedHeaders may have been stored with uppercase letters
        // during putBucketCors; ignore case when searching for match
        _matchesValue(allowedHeader.toLowerCase(), header));
}

/** _headersMatchRule - check if headers match AllowedHeaders of rule
* @param {string[]} headers - the value of the 'Access-Control-Request-Headers'
*   in an OPTIONS request
* @param {string[]} allowedHeaders - AllowedHeaders of a CORS rule
* @return {boolean} - true/false
*/
function _headersMatchRule(headers, allowedHeaders) {
    if (!allowedHeaders) {
        return false;
    }
    if (!headers.every(header => _matchesOneOf(allowedHeaders, header))) {
        return false;
    }
    return true;
}

/** _findCorsRule - Return first matching rule in cors rules that permits
*   CORS request
* @param {object[]} rules - array of rules
* @param {string} [rules.id] - optional id to identify rule
* @param {string[]} rules[].allowedMethods - methods allowed for CORS
* @param {string[]} rules[].allowedOrigins - origins allowed for CORS
* @param {string[]} [rules[].allowedHeaders] - headers allowed in an
*   OPTIONS request via the Access-Control-Request-Headers header
* @param {number} [rules[].maxAgeSeconds] - seconds browsers should cache
*   OPTIONS response
* @param {string[]} [rules[].exposeHeaders] - headers to expose to external
*   applications
* @param {string} origin - origin of CORS request
* @param {string} method - Access-Control-Request-Method header value in
*   an OPTIONS request and the actual method in any other request
* @param {string[]} [headers] - Access-Control-Request-Headers header value
*   in a preflight CORS request
* @return {(null|object)} - matching rule if found; null if no match
*/
function findCorsRule(rules, origin, method, headers) {
    return rules.find(rule => {
        if (rule.allowedMethods.indexOf(method) === -1) {
            return false;
        } else if (!rule.allowedOrigins.some(allowedOrigin =>
            _matchesValue(allowedOrigin, origin))) {
            return false;
        } else if (headers &&
            !_headersMatchRule(headers, rule.allowedHeaders)) {
            return false;
        }
        return true;
    });
}

/** _gatherResHeaders - Collect headers to return in response
* @param {object} rule - array of rules
* @param {string} [rule.id] - optional id to identify rule
* @param {string[]} rule[].allowedMethods - methods allowed for CORS
* @param {string[]} rule[].allowedOrigins - origins allowed for CORS
* @param {string[]} [rule[].allowedHeaders] - headers allowed in an
*   OPTIONS request via the Access-Control-Request-Headers header
* @param {number} [rule[].maxAgeSeconds] - seconds browsers should cache
*   OPTIONS response
* @param {string[]} [rule[].exposeHeaders] - headers to expose to external
*   applications
* @param {string} origin - origin of CORS request
* @param {string} method - Access-Control-Request-Method header value in
*   an OPTIONS request and the actual method in any other request
* @param {string[]} [headers] - Access-Control-Request-Headers header value
*   in a preflight CORS request
* @param {boolean} [isPreflight] - indicates if cors headers are being gathered
*   for a CORS preflight request
* @return {object} resHeaders - headers to include in response
*/
function generateCorsResHeaders(rule, origin, method, headers,
isPreflight) {
    const resHeaders = {
        'access-control-max-age': rule.maxAgeSeconds,
        'access-control-allow-methods': rule.allowedMethods.join(', '),
        'vary':
        'Origin, Access-Control-Request-Headers, Access-Control-Request-Method',
    };
    // send back '*' if any origin allowed; otherwise send back
    // request Origin value
    if (rule.allowedOrigins.indexOf('*') > -1) {
        resHeaders['access-control-allow-origin'] = '*';
    } else {
        resHeaders['access-control-allow-origin'] = origin;
        // can only set allow credentials to true if not returning wildcard
        // for 'access-control-allow-origin'
        resHeaders['access-control-allow-credentials'] = true;
    }
    if (headers) {
        resHeaders['access-control-allow-headers'] = headers.join(', ');
    }
    if (rule.exposeHeaders) {
        resHeaders['access-control-expose-headers'] =
            rule.exposeHeaders.join(', ');
    }
    if (isPreflight) {
        resHeaders['content-length'] = '0';
        resHeaders.date = new Date().toUTCString();
    }
    return resHeaders;
}

module.exports = {
    findCorsRule,
    generateCorsResHeaders,
};
