import crypto from 'crypto';

import { accountsKeyedbyAccessKey } from './vault.json';

export function hashSignature(stringToSign, secretKey, algorithm) {
    const hmacObject = crypto.createHmac(algorithm, secretKey);
    return hmacObject.update(stringToSign).digest('base64');
}

const vault = {
    authenticateV2Request: (accessKey, signatureFromRequest,
            stringToSign, log, callback) => {
        log.debug('Authenticating V2 Request');

        const account = accountsKeyedbyAccessKey[accessKey];
        if (!account) {
            log.error(`Invalid Access Key: ${accessKey}`);
            return callback('InvalidAccessKeyId');
        }
        const secretKey = account.secretKey;
        // If the signature sent is 43 characters,
        // this means that sha256 was used:
        // 43 characters in base64
        const algo = signatureFromRequest.length === 43 ?
            'sha256' : 'sha1';
        log.debug(`String to sign algorithm: ${algo}`);
        const reconstructedSig =
            hashSignature(stringToSign, secretKey, algo);
        log.debug(`Reconstructed signature: ${reconstructedSig}`);
        if (signatureFromRequest !== reconstructedSig) {
            log.error(`Signature does not match`);
            return callback('SignatureDoesNotMatch');
        }
        const userInfoToSend = {
            accountDisplayName: account.displayName,
            canonicalID: account.canonicalID,
            arn: account.arn,
            IAMdisplayName: account.IAMdisplayName,
        };
        log.debug(`User Info from Vault: ${JSON.stringify(userInfoToSend)}`);

        // For now, I am just sending back the canonicalID.
        // TODO: Refactor so that the accessKey information
        // passed to the API is the full accountInfo Object
        // rather than just the canonicalID string.
        // This is GH Issue#75
        return callback(null, userInfoToSend.canonicalID);
    }

};

export default vault;
