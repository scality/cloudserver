const assert = require('assert');

const { makeS3Request } = require('../utils/makeRequest');

const authCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};
const bucket = 'rawnodeapibucket';

describe('api tests', () => {
    before(done => {
        makeS3Request({
            method: 'PUT',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
        });
    });

    after(done => {
        makeS3Request({
            method: 'DELETE',
            authCredentials,
            bucket,
        }, err => {
            assert.ifError(err);
            done();
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
