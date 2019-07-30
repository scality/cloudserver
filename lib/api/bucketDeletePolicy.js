const { errors } = require('arsenal');

function bucketDeletePolicy(authInfo, request, log, callback) {
    return callback(errors.NotImplemented);
}

module.exports = bucketDeletePolicy;
