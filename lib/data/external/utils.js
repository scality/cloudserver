const utils = {
    logHelper(log, level, description, error, dataStoreName) {
        log[level](description, { error: error.message, stack: error.stack,
          dataStoreName });
    },
};

module.exports = utils;
