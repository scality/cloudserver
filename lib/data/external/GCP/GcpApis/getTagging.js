const { retrieveTags } = require('../GcpUtils');

function getObjectTagging(params, callback) {
    const headParams = {
        Bucket: params.Bucket,
        Key: params.Key,
        VersionId: params.VersionId,
    };
    this.headObject(headParams, (err, resObj) => {
        if (err) {
            return callback(err);
        }
        const TagSet = retrieveTags(resObj.Metadata);
        const retObj = {
            VersionId: resObj.VersionId,
            TagSet,
        };
        return callback(null, retObj);
    });
}

module.exports = getObjectTagging;
