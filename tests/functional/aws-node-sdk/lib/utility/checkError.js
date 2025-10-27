const assert = require('assert');

function checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    if (code) {
        assert.strictEqual(err.name, code);
    }
    if (statusCode) {
        assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
    }
}

module.exports = checkError;
