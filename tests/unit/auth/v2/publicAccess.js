import assert from 'assert';

import auth from '../../../../lib/auth/auth';
import DummyRequestLogger from '../../helpers.js';

const logger = new DummyRequestLogger();
describe('Public Access', () => {
    it('should grant access to a user that provides absolutely' +
        'no authentication information and should assign that user the ' +
        'All Users Group accessKey', done => {
        const request = {
            method: 'GET',
            lowerCaseHeaders: { host: 's3.amazonaws.com' },
            url: '/bucket',
            query: {},
        };

        auth(request, logger, (err, accessKey) => {
            assert.strictEqual(err, null);
            assert.strictEqual(accessKey,
                'http://acs.amazonaws.com/groups/global/AllUsers');
            done();
        });
    });

    it('should not grant access to a request that contains ' +
    'an authorization header without proper credentials', (done) => {
        const request = {
            method: 'GET',
            lowerCaseHeaders: {
                host: 's3.amazonaws.com',
                authorization: 'noAuth',
            },
            url: '/bucket',
            query: {},
        };

        auth(request, logger, err => {
            assert.strictEqual(err, 'MissingSecurityHeader');
            done();
        });
    });
});
