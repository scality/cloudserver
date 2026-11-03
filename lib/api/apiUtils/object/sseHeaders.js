const kms = require('../../../kms/wrapper');

function setSSEHeaders(headers, algo, kmsKey) {
    if (algo) {
        // eslint-disable-next-line no-param-reassign
        headers['x-amz-server-side-encryption'] = algo;
        if (kmsKey && algo === 'aws:kms') {
            // eslint-disable-next-line no-param-reassign
            headers['x-amz-server-side-encryption-aws-kms-key-id'] = kms.responseKeyId(kmsKey);
        }
    }
}

module.exports = {
    setSSEHeaders,
};
