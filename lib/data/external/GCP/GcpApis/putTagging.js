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
        next => this.headObject({
            Bucket: params.Bucket,
            Key: params.Key,
            VersionId: params.VersionId,
        }, next),
        (resObj, next) => {
            const completeMD = Object.assign({}, resObj.Metadata, tagRes);
            this.updateMetadata({
                Bucket: params.Bucket,
                Key: params.Key,
                VersionId: params.VersionId,
                Metadata: completeMD,
            }, next);
        },
    ], callback);
}

module.exports = putObjectTagging;
