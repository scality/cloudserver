const { retrieveTags } = require('../GcpUtils');

function getObjectTagging(params, callback) {
    const headParams = {
        Bucket: params.Bucket,
        Key: params.Key,
        VersionId: params.VersionId,
    };
    this.headObject(headParams, (err, res) => {
        const TagSet = retrieveTags(res.Metadata);
        const retObj = {
            VersionId: res.VersionId,
            TagSet,
        };
        return callback(null, retObj);
    });
}

module.exports = getObjectTagging;
