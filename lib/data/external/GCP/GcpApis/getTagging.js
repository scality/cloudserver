const async = require('async');

const { gcpTaggingPrefix } = require('../../../../../constants');

function getObjectTagging(params, callback) {
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
            const retObj = {
                VersionId: resObj.VersionId,
                TagSet: [],
            };
            Object.keys(resObj.Metadata).forEach(key => {
                if (key.startsWith(gcpTaggingPrefix)) {
                    retObj.TagSet.push({
                        Key: key.slice(gcpTaggingPrefix.length),
                        Value: resObj.Metadata[key],
                    });
                }
            });
            return next(null, retObj);
        },
    ], (err, result) => {
        if (err) {
            return callback(err);
        }
        return callback(null, result);
    });
}

module.exports = getObjectTagging;
