import { errors } from 'arsenal';
import assert from 'assert';

import auth from '../../../../lib/auth/auth';
import AuthInfo from '../../../../lib/auth/AuthInfo';
import constants from '../../../../constants';
import { DummyRequestLogger } from '../../helpers.js';

const logger = new DummyRequestLogger();
describe('Public Access', () => {
    it('should grant access to a user that provides absolutely' +
        'no authentication information and should assign that user the ' +
        'All Users Group accessKey', done => {
        const request = {
            method: 'GET',
            headers: { host: 's3.amazonaws.com' },
            url: '/bucket',
            query: {},
        };
        const publicAuthInfo = new AuthInfo({
            canonicalID: constants.publicId,
        });
        auth(request, logger, (err, authInfo) => {
            assert.strictEqual(err, null);
            assert.strictEqual(authInfo.getCanonicalID(),
                publicAuthInfo.getCanonicalID());
            done();
        });
    });

    it('should not grant access to a request that contains ' +
    'an authorization header without proper credentials', (done) => {
        const request = {
            method: 'GET',
            headers: {
                host: 's3.amazonaws.com',
                authorization: 'noAuth',
            },
            url: '/bucket',
            query: {},
        };

        auth(request, logger, err => {
            assert.deepStrictEqual(err, errors.MissingSecurityHeader);
            done();
        });
    });
});
