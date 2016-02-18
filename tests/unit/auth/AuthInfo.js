import assert from 'assert';

import AuthInfo from '../../../lib/auth/AuthInfo';
import constants from '../../../constants';

const arn = 'arn:aws:iam::123456789012:user/Fred';
const canonicalID = '123456789012123456789012123456789012';
const shortid = '123456789012';
const email = 'fred@auth.com';
const accountDisplayName = 'awesomeaccount';
const IAMdisplayName = 'Fred';

const infoFromVault = {
    arn,
    canonicalID,
    shortid,
    email,
    accountDisplayName,
    IAMdisplayName,
};
const authInfo = new AuthInfo(infoFromVault);

describe('AuthInfo class constructor', () => {
    it('should return an object', () => {
        assert.strictEqual(typeof authInfo, 'object');
    });

    it('should set properties', () => {
        assert.strictEqual(authInfo.arn, arn);
        assert.strictEqual(authInfo.canonicalID, canonicalID);
        assert.strictEqual(authInfo.shortid, shortid);
        assert.strictEqual(authInfo.email, email);
        assert.strictEqual(authInfo.accountDisplayName, accountDisplayName);
        assert.strictEqual(authInfo.IAMdisplayName, IAMdisplayName);
    });

    it('should have a working getArn() method', () => {
        assert.strictEqual(authInfo.getArn(), arn);
    });

    it('should have a working getCanonicalID() method', () => {
        assert.strictEqual(authInfo.getCanonicalID(), canonicalID);
    });

    it('should have a working getShortid() method', () => {
        assert.strictEqual(authInfo.getShortid(), shortid);
    });

    it('should have a working getEmail() method', () => {
        assert.strictEqual(authInfo.getEmail(), email);
    });

    it('should have a working getAccountDisplayName() method', () => {
        assert.strictEqual(authInfo.getAccountDisplayName(),
            accountDisplayName);
    });

    it('should have a working getIAMdisplayName() method', () => {
        assert.strictEqual(authInfo.getIAMdisplayName(), IAMdisplayName);
    });

    it('should have a working isRequesterAnIAMUser() method', () => {
        assert.strictEqual(authInfo.isRequesterAnIAMUser(), true);
        const accountUser = new AuthInfo({ canonicalID: 'account' });
        assert.strictEqual(accountUser.isRequesterAnIAMUser(), false);
    });

    it('should have a working isRequesterPublicUser() method', () => {
        assert.strictEqual(authInfo.isRequesterPublicUser(), false);
        const publicUser = new AuthInfo({ canonicalID: constants.publicId });
        assert.strictEqual(publicUser.isRequesterPublicUser(), true);
    });
});
