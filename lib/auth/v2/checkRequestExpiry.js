function checkRequestExpiry(timestamp) {
    // If timestamp is not within 15 minutes of current time, return true
    const currentTime = Date.now();
    const fifteenMinutes = (15 * 60 * 1000);
    if ((currentTime + fifteenMinutes) < timestamp) {
        return true;
    }
    return false;
}

export default checkRequestExpiry;
