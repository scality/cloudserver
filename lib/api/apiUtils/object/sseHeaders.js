const { config } = require('../../../Config');
const { getKeyIdFromArn } = require('arsenal/build/lib/network/KMSInterface');

function setSSEHeaders(headers, algo, kmsKey) {
    if (algo) {
        // eslint-disable-next-line no-param-reassign
        headers['x-amz-server-side-encryption'] = algo;
        if (kmsKey && algo === 'aws:kms') {
            // eslint-disable-next-line no-param-reassign
            headers['x-amz-server-side-encryption-aws-kms-key-id'] =
                config.kmsHideScalityArn ? getKeyIdFromArn(kmsKey) : kmsKey;
        }
    }
}

module.exports = {
    setSSEHeaders,
};
