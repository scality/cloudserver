const { timingSafeEqual } = require('crypto');

const { config } = require('../../../Config');

function isRateLimitServiceUser(authInfo, log) {
    try {
        const requestArn = authInfo.getArn();
        const configuredArn = config.rateLimiting?.serviceUserArn;

        if (!configuredArn) {
            log.warn('Access denied - no configured ARN under rateLimiting.serviceUserArn', { requestArn });
            return false;
        }

        // Support partial ARN matching (e.g., match by account only)
        // If configuredArn is shorter, do prefix match; otherwise exact match
        let match;
        if (requestArn.length >= configuredArn.length) {
            // Extract prefix from requestArn to match configuredArn length
            const requestPrefix = requestArn.substring(0, configuredArn.length);
            match = timingSafeEqual(
                Buffer.from(requestPrefix),
                Buffer.from(configuredArn)
            );
        } else {
            // requestArn is shorter than configured - no match
            log.warn('Access denied - request ARN is shorter than configured ARN', { requestArn, configuredArn });
            match = false;
        }
        return match;
    } catch (err) {
        log.error('Error checking if request is rate limit service user', { error: err });
        return false;
    }
}

module.exports = {
    isRateLimitServiceUser
};
