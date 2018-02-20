const async = require('async');

const { retrieveTags } = require('../GcpUtils');

function getObjectTagging(params, callback) {
    return async.waterfall([
        next => {
            const headParams = {
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
            };
            this.headObject(headParams, next);
        },
        (resObj, next) => {
            const TagSet = retrieveTags(resObj.Metadata);
            const retObj = {
                VersionId: resObj.VersionId,
                TagSet,
            };
            return next(null, retObj);
        },
    ], callback);
}

module.exports = getObjectTagging;
