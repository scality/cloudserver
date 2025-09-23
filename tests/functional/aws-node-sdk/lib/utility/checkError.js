const assert = require('assert');

function checkError(err, code, statusCode) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.Code, code);
    assert.strictEqual(err.$metadata.httpStatusCode, statusCode);
}

module.exports = checkError;
