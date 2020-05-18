const assert = require('assert');

const { makeS3Request } = require('../utils/makeRequest');

const authCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};
const bucket = 'rawnodeapibucket';

describe.only('api tests', () => {
    before(() => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
        });
    });

    after(() => {
        makeS3Request({
            method: 'DELETE',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
        });
    });

    it('should return 405 on headBucket when bucket is empty string', done => {
        makeS3Request({
            method: 'HEAD',
            authCredentials,
            bucket: '',
        }, (err, res) => {
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 405);
            return done();
        });
    });
});
