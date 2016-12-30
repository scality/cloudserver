
/**
 * findRoutingRule - find applicable routing rule from bucket metadata
 * @param {RoutingRule []} routingRules - array of routingRule objects
 * @param {string} key - object key
 * @param {number} [errCode] - error code to match if applicable
 * @return {object | undefined} redirectInfo -- comprised of all of the
 * keys/values from routingRule.getRedirect() plus
 * a key of prefixFromRule and a value of routingRule.condition.keyPrefixEquals
 */
export function findRoutingRule(routingRules, key, errCode) {
    if (!routingRules || routingRules.length === 0) {
        return undefined;
    }
    // For AWS compat:
    // 1) use first routing rules whose conditions are satisfied
    // 2) for matching prefix no need to check closest match.  first
    // match wins
    // 3) there can be a match for a key condition with and without
    // error code condition but first one that matches will be the rule
    // used. So, if prefix foo without error and first rule has error condition,
    // will fall through to next foo rule.  But if first foo rule has
    // no error condition, will have match on first rule even if later
    // there is more specific rule with error condition.
    for (let i = 0; i < routingRules.length; i++) {
        const prefixFromRule =
            routingRules[i].getCondition().keyPrefixEquals;
        const errorCodeFromRule =
            routingRules[i].getCondition().httpErrorCodeReturnedEquals;
        if (prefixFromRule !== undefined) {
            if (!key.startsWith(prefixFromRule)) {
                // no key match, move on
                continue;
            }
            // add the prefixFromRule to the redirect info
            // so we can replaceKeyPrefixWith if that is part of redirect
            // rule
            const redirectInfo = Object.assign({ prefixFromRule },
                routingRules[i].getRedirect());
            // have key match so check error code match
            if (errorCodeFromRule !== undefined) {
                if (errCode === errorCodeFromRule) {
                    return redirectInfo;
                }
                // if don't match on both conditions, this is not the rule
                // for us
                continue;
            }
            // if no error code condition at all, we have found our match
            return redirectInfo;
        }
        // we have an error code condition but no key condition
        if (errorCodeFromRule !== undefined) {
            if (errCode === errorCodeFromRule) {
                const redirectInfo = Object.assign({},
                    routingRules[i].getRedirect());
                return redirectInfo;
            }
            continue;
        }
        return undefined;
    }
    return undefined;
}

/**
 * extractRedirectInfo - convert location saved from x-amz-website header to
 * same format as redirectInfo saved from a put bucket website configuration
 * @param {string} location - location to redirect to
 * @return {object} redirectInfo - select key/values stored in
 * WebsiteConfiguration for a redirect -- protocol, replaceKeyWith and hostName
 */
export function extractRedirectInfo(location) {
    const redirectInfo = {};
    if (location.startsWith('/')) {
        // redirect to another object in bucket
        redirectInfo.replaceKeyWith = location.slice(1);
    } else if (location.startsWith('https')) {
        // otherwise, redirect to another website
        redirectInfo.protocol = 'https';
        redirectInfo.hostName = location.slice(8);
    } else {
        redirectInfo.protocol = 'http';
        redirectInfo.hostName = location.slice(7);
    }
    return redirectInfo;
}
