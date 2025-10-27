const assert = require('assert');
const { errors } = require('arsenal');

function assertError(err, expectedErr) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.Code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.Code}'`);
        assert.strictEqual(err.$metadata.httpStatusCode, errors[expectedErr].code,
            'incorrect error status code: should be  ' +
            `${errors[expectedErr].code}, but got '${err.$metadata.httpStatusCode}'`);
    }
}

module.exports = assertError;
