import assert from 'assert';

import auth from '../../../../lib/auth/auth';
import { DummyRequestLogger } from '../../helpers';

const logger = new DummyRequestLogger();

describe('Error handling in checkAuth', () => {
    it('should return an error message if no ' +
       'secret key is associated with access key', done => {
        const date = new Date();
        const request = {
            method: 'GET',
            lowerCaseHeaders: {
                host: 's3.amazonaws.com',
                'user-agent': 'curl/7.43.0',
                accept: '*/*',
                date,
                authorization: 'AWS brokenKey1:MJNF7AqNapSu32TlBOVkcAxj58c=',
            },
            url: '/bucket',
            query: {},
        };
        auth(request, logger, err => {
            assert.strictEqual(err, 'InvalidArgument');
            done();
        });
    });

    it('should return an error message if no date header ' +
       'is provided with v2header auth check', done => {
        const request = {
            method: 'GET',
            lowerCaseHeaders: {
                host: 's3.amazonaws.com',
                'user-agent': 'curl/7.43.0',
                accept: '*/*',
                authorization:
                    'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c='
            },
            url: '/bucket',
        };

        auth(request, logger, err => {
            assert.strictEqual(err, 'MissingSecurityHeader');
            done();
        });
    });

    it('should return an error message if the Expires ' +
       'query parameter is more than 15 minutes ' +
       'old with query auth check', done => {
        const request = {
            method: 'GET',
            url: '/bucket?AWSAccessKeyId=accessKey1&' +
                'Expires=1141889120&Signature=' +
                'vjbyPxybdZaNmGa%2ByT272YEAiv4%3D',
            query: {
                AWSAccessKeyId: 'accessKey1',
                Expires: '1141889120',
                Signature: 'vjbyPxybdZaNmGa%2ByT272YEAiv4%3D'
            },
            lowerCaseHeaders: {},
            headers: {}
        };
        auth(request, logger, err => {
            assert.strictEqual(err, 'RequestTimeTooSkewed');
            done();
        });
    });

    it('should return an error message if ' +
       'the signatures do not match for v2query auth', done => {
        // Date.now() provides milliseconds since 1/1/1970.
        // AWS Expires is in seconds so need to divide by 1000
        let expires = Date.now() / 1000;
        const fifteenMinutes = (15 * 60);
        expires = expires + fifteenMinutes;
        const request = {
            method: 'GET',
            url: `/bucket?AWSAccessKeyId=accessKey1&Expires` +
                `=${expires}&Signature=vjbyPxybdZaNmGa` +
                `%2ByT272YEAiv4%3D`,
            query: {
                AWSAccessKeyId: 'accessKey1',
                Expires: expires,
                Signature: 'vjbyPxybdZaNmGa%2ByT272YEAiv4%3D' },
            lowerCaseHeaders: { host: 's3.amazonaws.com' },
        };

        auth(request, logger, err => {
            assert.strictEqual(err, 'SignatureDoesNotMatch');
            done();
        });
    });

    it('should return an error message if the ' +
       'signatures do not match for v2header auth', done => {
        const date = new Date();
        const request = {
            method: 'GET',
            lowerCaseHeaders: {
                host: 's3.amazonaws.com',
                'user-agent': 'curl/7.43.0',
                accept: '*/*',
                date,
                authorization: 'AWS accessKey1:MJNF7AqNapSu32TlBOVkcAxj58c=',
            },
            url: '/bucket',
            query: {}
        };

        auth(request, logger, err => {
            assert.strictEqual(err, 'SignatureDoesNotMatch');
            done();
        });
    });
});
