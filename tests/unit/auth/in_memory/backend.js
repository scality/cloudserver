const assert = require('assert');

const { buildAuthDataAccount } =
    require('../../../../lib/auth/in_memory/builder');

const fakeAccessKey = 'fakeaccesskey';
const fakeSecretKey = 'fakesecretkey';
const fakeCanonicalId = 'fakecanonicalid';
const fakeServiceName = 'fakeservicename';
const fakeUserName = 'fakeusername';

const defaultUserName = 'CustomAccount';

function getFirstAndOnlyAccount(authdata) {
    return authdata.accounts[0];
}

describe('buildAuthDataAccount function', () => {
    it('should return authdata with the default user name if no user ' +
    'name provided', () => {
        const authdata = buildAuthDataAccount(fakeAccessKey, fakeSecretKey,
            fakeCanonicalId, fakeServiceName);
        const firstAccount = getFirstAndOnlyAccount(authdata);
        assert.strictEqual(firstAccount.name, defaultUserName);
    });

    it('should return authdata with the user name that has been ' +
    'provided', () => {
        const authdata = buildAuthDataAccount(fakeAccessKey, fakeSecretKey,
            fakeCanonicalId, fakeServiceName, fakeUserName);
        const firstAccount = getFirstAndOnlyAccount(authdata);
        assert.strictEqual(firstAccount.name, fakeUserName);
    });
});
