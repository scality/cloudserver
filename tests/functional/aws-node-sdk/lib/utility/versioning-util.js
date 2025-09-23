const assert = require('assert');
const { 
    S3Client, 
    ListObjectVersionsCommand, 
    DeleteObjectsCommand, 
    PutBucketVersioningCommand, 
    PutObjectCommand, 
    DeleteObjectCommand, 
    GetObjectCommand 
} = require('@aws-sdk/client-s3');

const getConfig = require('../../test/support/config');
const config = getConfig('default');
const s3 = new S3Client(config);

const versioningEnabled = { Status: 'Enabled' };
const versioningSuspended = { Status: 'Suspended' };

async function _deleteVersionList(versionList, bucket) {
    if (versionList === undefined || versionList.length === 0) {
        return null;
    }
    const params = { Bucket: bucket, Delete: { Objects: [] } };
    versionList.forEach(version => {
        params.Delete.Objects.push({
            Key: version.Key, VersionId: version.VersionId });
    });

    return await s3.send(new DeleteObjectsCommand(params));
}

async function checkOneVersion(s3Client, bucket, versionId) {
    const data = await s3Client.send(new ListObjectVersionsCommand({ Bucket: bucket }));
    assert.strictEqual(data.Versions.length, 1);
    if (versionId) {
        assert.strictEqual(data.Versions[0].VersionId, versionId);
    }
    assert.strictEqual(data.DeleteMarkers, undefined);
}

async function removeAllVersions(params) {
    const bucket = params.Bucket;
    
    try {
        const command = new ListObjectVersionsCommand(params);
        const data = await s3.send(command);
        if (data.DeleteMarkers && data.DeleteMarkers.length > 0) {
            await _deleteVersionList(data.DeleteMarkers, bucket);
        }
        if (data.Versions && data.Versions.length > 0) {
            await _deleteVersionList(data.Versions, bucket);
        }
        
        if (data.IsTruncated) {
            const nextParams = {
                Bucket: bucket,
                KeyMarker: data.NextKeyMarker,
                VersionIdMarker: data.NextVersionIdMarker,
            };
            return removeAllVersions(nextParams);
        }
        return null;
    } catch (error) {
        return error;
    }
}

async function suspendVersioning(bucket) {
    return s3.send(new PutBucketVersioningCommand({
        Bucket: bucket,
        VersioningConfiguration: versioningSuspended,
    }));
}

async function enableVersioning(bucket) {
    return s3.send(new PutBucketVersioningCommand({
        Bucket: bucket,
        VersioningConfiguration: versioningEnabled,
    }));
}

async function enableVersioningThenPutObject(bucket, object) {
    await enableVersioning(bucket);
    return s3.send(new PutObjectCommand({ Bucket: bucket, Key: object }));
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
 *  @param {S3Client} s3Client - aws sdk v3 s3 client instance
 *  @param {string} bucketName - name of bucket in versioning suspended state
 *  @param {string} keyName - name of key
 *  @return {Promise} - promise that resolves when complete
 */
async function createDualNullVersion(s3Client, bucketName, keyName) {
    // put null version
    await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: keyName }));
    
    await enableVersioning(bucketName);
    
    // should store null version as separate version before
    // putting new version
    const data = await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: keyName }));
    assert(data.VersionId, 'Expected VersionId in response');
    
    // delete version we just created, master version should be updated
    // with value of next most recent version: null version previously put
    await s3Client.send(new DeleteObjectCommand({
        Bucket: bucketName,
        Key: keyName,
        VersionId: data.VersionId,
    }));
    
    // getting object should return null version now
    const getResult = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: keyName }));
    assert.strictEqual(getResult.VersionId, 'null', 'Expected null version');
    return null;
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
