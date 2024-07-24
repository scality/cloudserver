const memCredentials = require('../../lib/json/mem_credentials.json');

if (!memCredentials || Object.is(memCredentials, {})) {
    throw new Error('Credential info is missing in mem_credentials.json');
}

function getCredentials(profile = 'default') {
    const credentials = memCredentials[profile] || memCredentials.default;

    const accessKeyId = credentials.accessKey;
    const secretAccessKey = credentials.secretKey;

    return {
        accessKeyId,
        secretAccessKey,
    };
}

module.exports = {
    getCredentials,
};
