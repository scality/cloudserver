function checkExpectedBucketOwner(headers, bucket, log, cb) {
    const expectedOwner = headers['x-amz-expected-bucket-owner'];
    if (expectedOwner === undefined) {
        return cb();
    }

    const bucketOwner = bucket.getOwner();
    return vault.getAccountIds([bucketOwner], log, (error, res) => {
        if (error) {
            log.error('error fetch accountId from vault', {
                method: 'checkExpectedBucketOwner',
                error,
            });
        }

        if (error || res[bucketOwner] !== expectedOwner) {
            return cb(errors.AccessDenied);
        }

        return cb();
    });
}

module.exports = {
    checkExpectedBucketOwner,
};
