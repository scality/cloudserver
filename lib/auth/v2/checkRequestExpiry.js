function checkRequestExpiry(timestamp, log) {
    // If timestamp is not within 15 minutes of current time, or if
    // timestamp is more than 15 minutes in the future, the request
    // has expired and return true
    const currentTime = Date.now();
    log.trace('request timestamp', { requestTimestamp: timestamp });
    log.trace('current timestamp', {currentTimestamp: currentTime });
    const fifteenMinutes = (15 * 60 * 1000);
    if (currentTime - timestamp > fifteenMinutes) {
        log.trace('request timestamp is not within 15 minutes of current time');
        return true;
    }

    if (currentTime + fifteenMinutes < timestamp) {
        log.trace('request timestamp is more than 15 minutes into future');
        return true;
    }

    return false;
}

export default checkRequestExpiry;
