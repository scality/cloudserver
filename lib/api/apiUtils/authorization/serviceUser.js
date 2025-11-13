const { timingSafeEqual } = require('crypto');

const { config } = require('../../../Config');

function isRateLimitServiceUser(authInfo) {
    try {
        console.log('authInfo.getArn()', authInfo.getArn());
        console.log('config.rateLimiting.serviceUserArn', config.rateLimiting.serviceUserArn);
        return timingSafeEqual(Buffer.from(authInfo.getArn()), Buffer.from(config.rateLimiting.serviceUserArn));
    } catch {
        return false;
    }
}

module.exports = {
    isRateLimitServiceUser
};
