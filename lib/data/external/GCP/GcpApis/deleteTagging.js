const async = require('async');

const { stripTags } = require('../GcpUtils');

function deleteObjectTagging(params, callback) {
    return async.waterfall([
        next => {
            const taggingParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
            };
            this.headObject(taggingParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                return next(null, res);
            });
        },
        (resObj, next) => {
            const completeMD = stripTags(resObj.Metadata);
            return next(null, completeMD);
        },
        (completeMD, next) => {
            const taggingParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
                Metadata: completeMD,
            };
            this.updateMetadata(taggingParams, err => next(err));
        },
    ], err => callback(err));
}
module.exports = deleteObjectTagging;
