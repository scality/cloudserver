const { makeGcpRequest } = require('./makeRequest');

function gcpRequestRetry(params, retry, callback) {
    const retryTimeout = {
        0: 0,
        1: 1000,
        2: 2000,
        3: 4000,
        4: 8000,
    };
    const maxRetries = 4;
    const timeout = retryTimeout[retry];
    return setTimeout(makeGcpRequest, timeout, params, (err, res) => {
        if (err) {
            if (retry <= maxRetries && err.statusCode === 429) {
                return gcpRequestRetry(params, retry + 1, callback);
            }
            return callback(err);
        }
        return callback(null, res);
    });
}

module.exports = {
    gcpRequestRetry,
};
