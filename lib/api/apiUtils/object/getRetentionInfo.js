function getRetentionInfo(bucketMD) {
    const objLockConfig = bucketMD.getObjectLockConfiguration();

    const objRetention = {};
    if (objLockConfig) {
        objRetention.mode = objLockConfig.rule.mode;
        const date = new Date();
        const day = objLockConfig.rule.day;
        if (day) {
            date.setDate(date.getDate() + day);
        } else {
            date.setFullYear(date.getFullYear() + objLockConfig.rule.year);
        }
        objRetention.retentionUntilDate = date;
        return objRetention;
    }
    return undefined;
}

module.exports = getRetentionInfo;
