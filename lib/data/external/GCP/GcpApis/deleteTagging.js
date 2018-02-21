const async = require('async');

const { stripTags } = require('../GcpUtils');

function deleteObjectTagging(params, callback) {
    return async.waterfall([
        next => this.headObject({
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: params.VersionId,
        }, next),
        (resObj, next) => {
            const completeMD = stripTags(resObj.Metadata);
            this.updateMetadata({
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
                Metadata: completeMD,
            }, next);
        },
    ], callback);
}
module.exports = deleteObjectTagging;
