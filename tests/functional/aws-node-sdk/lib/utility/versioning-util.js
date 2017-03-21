import async from 'async';
import { S3 } from 'aws-sdk';

import getConfig from '../../test/support/config';
const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

export const constants = {
    versioningEnabled: { Status: 'Enabled' },
    versioningSuspended: { Status: 'Suspended' },
};

function _deleteVersionList(versionList, bucket, callback) {
    async.each(versionList, (versionInfo, cb) => {
        const versionId = versionInfo.VersionId;
        const params = { Bucket: bucket, Key: versionInfo.Key,
        VersionId: versionId };
        s3.deleteObject(params, cb);
    }, callback);
}

export function removeAllVersions(params, callback) {
    const bucket = params.Bucket;
    async.waterfall([
        cb => s3.listObjectVersions(params, cb),
        (data, cb) => _deleteVersionList(data.DeleteMarkers, bucket,
            err => cb(err, data)),
        (data, cb) => _deleteVersionList(data.Versions, bucket,
            err => cb(err, data)),
        (data, cb) => {
            if (data.IsTruncated) {
                const params = {
                    Bucket: bucket,
                    KeyMarker: data.NextKeyMarker,
                    VersionIdMarker: data.NextVersionIdMarker,
                };
                return removeAllVersions(params, cb);
            }
            return cb();
        },
    ], callback);
}
