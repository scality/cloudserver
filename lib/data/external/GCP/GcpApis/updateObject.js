const async = require('async');
const { errors } = require('arsenal');

function updateObject(params, cb) {
    const { Bucket, Key, Metadata } = params;
    return async.waterfall([
        next => this.headObject({ Bucket, Key }, next),
        (res, next) => this.copyObject({
            Bucket,
            Key,
            CopySource: `${Bucket}/${Key}`,
            Metadata: Object.assign({}, res.Metadata, Metadata),
            MetadataDirective: 'REPLACE',
        }, next),
    ], cb);
}

module.exports = updateObject;
