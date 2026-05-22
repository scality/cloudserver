const { timingSafeEqual } = require('crypto');

const { config } = require('../../../Config');

function isRateLimitServiceUser(authInfo) {
    try {
        return timingSafeEqual(Buffer.from(authInfo.getArn()), Buffer.from(config.rateLimiting.serviceUserArn));
    } catch {
        return false;
    }
}

module.exports = {
    isRateLimitServiceUser,
};
