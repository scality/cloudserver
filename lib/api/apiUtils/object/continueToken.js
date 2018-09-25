/**
 * generateToken - generates obfuscated continue token from object keyName
 * @param {String} keyName - name of key to obfuscate
 * @return {String} - obfuscated continue token
 */
function generateToken(keyName) {
    if (keyName === '' || keyName === undefined) {
        return undefined;
    }
    return Buffer.from(keyName).toString('base64');
}

/**
 * decryptToken - decrypts object keyName from obfuscated continue token
 * @param {String} token - obfuscated continue token
 * @return {String} - object keyName
 */
function decryptToken(token) {
    if (token === '' || token === undefined) {
        return undefined;
    }
    return Buffer.from(token, 'base64').toString('utf8');
}

module.exports = {
    generateToken,
    decryptToken,
};
