const assert = require('assert');

const { makeRequest } = require('../../utils/makeRequest');
const { getCredentials } = require('../../../aws-node-sdk/test/support/credentials');

const ipAddress = process.env.IP ? process.env.IP : 'localhost';
const { accessKeyId, secretAccessKey } = getCredentials();
const authCredentials = { accessKey: accessKeyId, secretKey: secretAccessKey };

// The router gives these answers before it reads the destination API flag.
// Thus the answers are the same when the flag is on or off. The test never
// creates the bucket, because the router answers before any bucket lookup.
const bucket = 'notification-destination-routes';

function makeDestinationRequest(method, path, callback) {
    makeRequest(
        {
            authCredentials,
            hostname: ipAddress,
            port: 8000,
            method,
            path,
            queryObj: { 'notification-destination': '' },
        },
        callback,
    );
}

describe('notification destination routes', () => {
    ['GET', 'PUT', 'DELETE'].forEach(method => {
        it(`should answer MethodNotAllowed to ${method} on a bucket`, done => {
            makeDestinationRequest(method, `/${bucket}`, (err, res) => {
                assert(err, 'expected an error');
                assert.strictEqual(err.code, 'MethodNotAllowed');
                assert.strictEqual(err.statusCode, 405);
                assert.strictEqual(res.statusCode, 405);
                done();
            });
        });
    });

    it('should answer 405 to HEAD on a bucket', done => {
        makeDestinationRequest('HEAD', `/${bucket}`, (err, res) => {
            // A HEAD answer has no body, so there is no error to parse from it.
            assert.ifError(err);
            assert.strictEqual(res.statusCode, 405);
            done();
        });
    });

    ['PUT', 'POST', 'DELETE'].forEach(method => {
        it(`should answer InvalidArgument to ${method} on the service with no name`, done => {
            makeDestinationRequest(method, '/', (err, res) => {
                assert(err, 'expected an error');
                assert.strictEqual(err.code, 'InvalidArgument');
                assert.strictEqual(err.statusCode, 400);
                assert.strictEqual(res.statusCode, 400);
                done();
            });
        });
    });
});
