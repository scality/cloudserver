/* eslint-disable */

const assert = require('assert');

module.exports = function (params, callback) {
    assert.strictEqual('object', typeof params.Records[0].s3);
    const s3 = params.Records[0].s3;
    assert.strictEqual(s3.object.size, Buffer.byteLength(s3.object.data));
    callback(null, 'blah');
};
