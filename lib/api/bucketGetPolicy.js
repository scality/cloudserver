const { errors } = require('arsenal');

function bucketGetPolicy(authInfo, request, log, callback) {
    return callback(errors.NotImplemented);
}

module.exports = bucketGetPolicy;
