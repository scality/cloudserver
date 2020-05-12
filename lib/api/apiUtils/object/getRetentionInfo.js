function getRetentionInfo(bucketMD) {
    const objLockConfig = bucketMD.getObjectLockConfiguration();

    const objRetention = {};
    if (objLockConfig) {
        objRetention
        return objRetention;
    }
    return undefined;
}

module.exports = getRetentionInfo;
