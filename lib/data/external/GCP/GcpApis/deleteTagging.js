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
            this.copyObject({
                Bucket: params.Bucket,
                Key: params.Key,
                CopySource: `${params.Bucket}/${params.Key}`,
                Metadata: completeMD,
                MetadataDirective: 'REPLACE',
            }, next);
        },
    ], callback);
}
module.exports = deleteObjectTagging;
