const metadata = require('../../../lib/metadata/wrapper');


function templateSSEConfig({ algorithm, keyId }) {
    const xml = [];
    xml.push(`
    <?xml version="1.0" encoding="UTF-8"?>
    <ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Rule>
            <ApplyServerSideEncryptionByDefault>`
    );

    if (algorithm) {
        xml.push(`<SSEAlgorithm>${algorithm}</SSEAlgorithm>`);
    }

    if (keyId) {
        xml.push(`<KMSMasterKeyID>${keyId}</KMSMasterKeyID>`);
    }

    xml.push(`</ApplyServerSideEncryptionByDefault>
        </Rule>
    </ServerSideEncryptionConfiguration>`);
    return xml.join('');
}

function templateRequest(bucketName, { post }) {
    return {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        post,
    };
}

function getSSEConfig(bucketName, log, cb) {
    return metadata.getBucket(bucketName, log, (err, md) => {
        if (err) {
            return cb(err);
        }
        return cb(null, md.getServerSideEncryption());
    });
}

module.exports = {
    templateRequest,
    templateSSEConfig,
    getSSEConfig,
};
