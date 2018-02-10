const async = require('async');
const { errors } = require('arsenal');

const { processTagSet } = require('../GcpUtils');

function putObjectTagging(params, callback) {
    if (!params.Tagging || !params.Tagging.TagSet) {
        return callback(errors.MissingParameter);
    }
    const tagRes = processTagSet(params.Tagging.TagSet);
    if (tagRes instanceof Error) {
        return callback(tagRes);
    }
    return async.waterfall([
        next => {
            const headParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
            };
            this.headObject(headParams, (err, res) => {
                if (err) {
                    return next(err);
                }
                return next(null, res);
            });
        },
        (resObj, next) => {
            const currentMD = Object.assign({}, resObj.Metadata);
            const completeMD = Object.assign(currentMD, tagRes);
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

module.exports = putObjectTagging;
