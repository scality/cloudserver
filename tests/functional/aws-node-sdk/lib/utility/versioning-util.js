import async from 'async';
import assert from 'assert';
import { S3 } from 'aws-sdk';

import getConfig from '../../test/support/config';
const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

export const versioningEnabled = { Status: 'Enabled' };
export const versioningSuspended = { Status: 'Suspended' };

function _deleteVersionList(versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const params = { Bucket: bucket, Delete: { Objects: [] } };
    versionList.forEach(version => {
        params.Delete.Objects.push({
            Key: version.Key, VersionId: version.VersionId });
    });

    return s3.deleteObjects(params, callback);
}

export function checkOneVersion(data, versionId) {
    assert.strictEqual(data.Versions.length, 1);
    assert.strictEqual(data.Versions[0].VersionId, versionId);
    assert.strictEqual(data.DeleteMarkers.length, 0);
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
