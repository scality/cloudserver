/* eslint-disable */

const assert = require('assert');
const stream = require('stream');

module.exports = function (params, callback) {
    assert.strictEqual('object', typeof params.Records[0].s3);
    const s3 = params.Records[0].s3;
    assert(s3.object.body instanceof stream.Duplex);
    s3.object.metadata['simple-handler'] = 'passed';
    s3.object.contentType = 'text/plain';
    s3.object.key = 'food.txt';
    callback(null);
};
