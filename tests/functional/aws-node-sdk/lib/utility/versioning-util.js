const async = require('async');
const assert = require('assert');
const { S3Client, 
    ListObjectVersionsCommand, 
    GetObjectCommand,
    DeleteObjectsCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteObjectCommand } = require('@aws-sdk/client-s3');

const getConfig = require('../../test/support/config');
const config = getConfig('default');
const s3Client = new S3Client(config);

const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };

function _deleteVersionList(versionList, bucket, callback) {
    if (versionList === undefined || versionList.length === 0) {
        return callback();
    }
    const params = { Bucket: bucket, Delete: { Objects: [] } };
    versionList.forEach(version => {
        params.Delete.Objects.push({
            Key: version.Key, VersionId: version.VersionId });
    });

    return s3Client.send(new DeleteObjectsCommand(params)).then(() => callback()).catch(err => callback(err));
}

async function checkOneVersion(s3, bucket, versionId) {
    return await s3Client.send(new ListObjectVersionsCommand({ Bucket: bucket })).then(data => {
            assert.strictEqual(data.Versions.length, 1);
            if (versionId) {
                assert.strictEqual(data.Versions[0].VersionId, versionId);
            }
            assert.strictEqual(data.DeleteMarkers?.length, undefined);
        });
}

function removeAllVersions(params, callback) {
    const bucket = params.Bucket;
    async.waterfall([
        cb => s3Client.send(new ListObjectVersionsCommand(params)).then(data =>
            cb(null, data)).catch(err => cb(err)),
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

function suspendVersioning(bucket, callback) {
    s3Client.send(new PutBucketVersioningCommand({
        Bucket: bucket,
        VersioningConfiguration: versioningSuspended,
    })).then(() => callback()).catch(err => callback(err));
}

function enableVersioning(bucket, callback) {
    s3Client.send(new PutBucketVersioningCommand({
        Bucket: bucket,
        VersioningConfiguration: versioningEnabled,
    })).then(() => callback()).catch(err => callback(err));
}

function enableVersioningThenPutObject(bucket, object, callback) {
    enableVersioning(bucket, err => {
        if (err) {
            callback(err);
        }
        s3Client.send(new PutObjectCommand({ Bucket: bucket, Key: object, Body: '' })).then(() => 
            callback()).catch(err => callback(err));
    });
}

/** createDualNullVersion
 *
 * - PREVIOUSLY: created a null version that was stored in metadata
 *   both in the master version and a separate version
 *
 * - CURRENTLY: only one null version key is present in metadata
 *   after the second put, as the first null key is cleaned up
 *
 *  Even though there is only one key at the end, it is still useful
 *  to keep this function for regression testing
 *
 *  @param {AWS.S3} s3 - aws sdk s3 instance
 *  @param {string} bucketName - name of bucket in versioning suspended state
 *  @param {string} keyName - name of key
 *  @param {callback} cb - callback
 *  @return {undefined} - and call callback
 */
function createDualNullVersion(s3, bucketName, keyName, cb) {
    async.waterfall([
        // put null version
        next => s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: keyName, Body: '' })).then(() => 
            next()).catch(err => next(err)),
        next => enableVersioning(bucketName, err => next(err)),
        // should store null version as separate version before
        // putting new version
        next => s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: keyName, Body: '' })).then(data => {
            assert(data.VersionId);
            next(null, data.VersionId);
        }).catch(err => {
            assert.strictEqual(err, null,
                'Unexpected err putting new version');
            next(err);
        }),
        // delete version we just created, master version should be updated
        // with value of next most recent version: null version previously put
        (versionId, next) => s3Client.send(new DeleteObjectCommand({
            Bucket: bucketName,
            Key: keyName,
            VersionId: versionId,
        })).then(() => next()).catch(err => next(err)),
        // getting object should return null version now
        next => s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: keyName })).then(data => {
            assert.strictEqual(data.VersionId, 'null');
            next();
        }).catch(err => {
            assert.strictEqual(err, null,
                'Unexpected err getting latest version');
            next(err);
        }),
    ], err => cb(err));
}

module.exports = {
    checkOneVersion,
    versioningEnabled,
    versioningSuspended,
    suspendVersioning,
    removeAllVersions,
    enableVersioningThenPutObject,
    createDualNullVersion,
};
