const assert = require('assert');

module.exports = (event, context, callback) => {
    assert.strictEqual('object', typeof event.Records[0].s3);
    const s3 = event.Records[0].s3;
    assert.strictEqual(null, s3.object.body);
    s3.object.metadata['retrieve-handler'] = 'passed';
    s3.object.contentType = 'text/plain';
    s3.object.key = 'food.txt';
    callback(null);
};
