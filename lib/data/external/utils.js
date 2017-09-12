const utils = {
    logHelper(log, level, description, error, dataStoreName) {
        log[level](description, { error: error.message, stack: error.stack,
            dataStoreName });
    },
    // take off the 'x-amz-meta-'
    trimXMetaPrefix(obj) {
        const newObj = {};
        Object.keys(obj).forEach(key => {
            const newKey = key.substring(11);
            newObj[newKey] = obj[key];
        });
        return newObj;
    },
};

module.exports = utils;
