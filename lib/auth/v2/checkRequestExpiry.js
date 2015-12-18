function checkRequestExpiry(timestamp) {
    // If timestamp is not within 15 minutes of current time, or if
    // timestamp is more than 15 minutes in the future, the request
    // has expired and return true
    const currentTime = Date.now();
    const fifteenMinutes = (15 * 60 * 1000);
    if ((currentTime - timestamp) > fifteenMinutes ||
        (currentTime + fifteenMinutes) < timestamp) {
        return true;
    }
    return false;
}

export default checkRequestExpiry;
