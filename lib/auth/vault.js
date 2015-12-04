import crypto from 'crypto';

import { accountsKeyedbyAccessKey } from './vault.json';

export function hashSignature(stringToSign, secretKey, algorithm) {
    const hmacObject = crypto.createHmac(algorithm, secretKey);
    return hmacObject.update(stringToSign).digest('base64');
}

const vault = {
    authenticateV2Request: (accessKey, signatureFromRequest,
            stringToSign, callback) => {
        const account = accountsKeyedbyAccessKey[accessKey];
        if (!account) {
            return callback('InvalidAccessKeyId');
        }
        const secretKey = account.secretKey;
        const reconstructedSignature =
            hashSignature(stringToSign, secretKey, 'sha1');
        if (reconstructedSignature !== signatureFromRequest) {
            return callback('SignatureDoesNotMatch');
        }
        const userInfoToSend = {
            accountDisplayName: account.displayName,
            canonicalID: account.canonicalID,
            arn: account.arn,
            IAMdisplayName: account.IAMdisplayName,
        };

        // For now, I am just sending back the canonicalID.
        // TODO: Refactor so that the accessKey information
        // passed to the API is the full accountInfo Object
        // rather than just the canonicalID string.
        // This is GH Issue#75
        return callback(null, userInfoToSend.canonicalID);
    }

};

export default vault;
