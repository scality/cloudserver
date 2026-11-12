const assert = require('assert');
const AuthInfo = require('@scality/arsenal').auth.AuthInfo;

const { requesterAccountId } = require('../../../../../lib/api/apiUtils/notificationDestination/request');
const { makeAuthInfo } = require('../../../helpers');

// the account of accessKey1 in makeAuthInfo
const accountId = '123456789098';

describe('notification destination request helpers', () => {
    describe('requesterAccountId', () => {
        it('should return the account of an account', () => {
            assert.strictEqual(requesterAccountId(makeAuthInfo('accessKey1')), accountId);
        });

        // Vault gives an IAM user or a role a short ID that is not the
        // account ID. makeAuthInfo gives a user the short ID of its account.
        [
            ['an IAM user', `arn:aws:iam::${accountId}:user/alice`, 'AIDAUSERID0000000000'],
            ['an assumed role', `arn:aws:sts::${accountId}:assumed-role/r/s`, 'AROAROLEID0000000000:s'],
        ].forEach(([desc, arn, shortid]) => {
            it(`should return the account of ${desc}, never its short ID`, () => {
                assert.strictEqual(requesterAccountId(new AuthInfo({ shortid, arn })), accountId);
            });
        });
    });
});
