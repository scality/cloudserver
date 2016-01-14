function checkRequestExpiry(timestamp, log) {
    // If timestamp is not within 15 minutes of current time, or if
    // timestamp is more than 15 minutes in the future, the request
    // has expired and return true
    const currentTime = Date.now();
    log.debug(`Request Timestamp: ${timestamp}`);
    log.debug(`Current Timestamp: ${currentTime}`);
    const fifteenMinutes = (15 * 60 * 1000);
    if (currentTime - timestamp > fifteenMinutes) {
        log.debug('Request timestamp is not within 15 minutes of current time');
        return true;
    }

    if (currentTime + fifteenMinutes < timestamp) {
        log.debug('Request timestamp is more than 15 minutes into future');
        return true;
    }

    return false;
}

export default checkRequestExpiry;
