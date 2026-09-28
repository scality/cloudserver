const {
    S3Client,
    HeadBucketCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    ListObjectVersionsCommand,
    DeleteObjectsCommand,
    DeleteObjectCommand,
    ListBucketsCommand,
} = require('@aws-sdk/client-s3');
const projectFixture = require('../fixtures/project');
const getConfig = require('../../test/support/config');

class BucketUtility {
    constructor(profile = 'default', config = {}, unauthenticated = false) {
        const s3Config = getConfig(profile, config);
        if (unauthenticated) {
            this.s3 = new S3Client({
                ...s3Config,
                maxAttempts: 0,
                credentials: { accessKeyId: '', secretAccessKey: '' },
                forcePathStyle: true,
                signer: { sign: async request => request },
            });
        } else {
            this.s3 = new S3Client({
                ...s3Config,
                maxAttempts: 0,
            });
        }
    }

    bucketExists(bucketName) {
        return this.s3
            .send(new HeadBucketCommand({ Bucket: bucketName }))
            .then(() => true)
            .catch(err => {
                if (err.name === 'NotFound') {
                    return false;
                }
                throw err;
            });
    }

    createOne(bucketName) {
        return this.s3
            .send(new CreateBucketCommand({ Bucket: bucketName }))
            .then(() => bucketName)
            .catch(err => {
                throw err;
            });
    }

    createOneWithLock(bucketName) {
        return this.s3
            .send(
                new CreateBucketCommand({
                    Bucket: bucketName,
                    ObjectLockEnabledForBucket: true,
                }),
            )
            .then(() => bucketName);
    }

    createMany(bucketNames) {
        const promises = bucketNames.map(bucketName => this.createOne(bucketName));
        return Promise.all(promises);
    }

    createRandom(nBuckets = 1) {
        if (nBuckets === 1) {
            const bucketName = projectFixture.generateBucketName();
            return this.createOne(bucketName);
        }
        const bucketNames = projectFixture.generateManyBucketNames(nBuckets).sort(() => 0.5 - Math.random());
        return this.createMany(bucketNames);
    }

    deleteOne(bucketName) {
        return this.s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    }

    deleteMany(bucketNames) {
        const promises = bucketNames.map(bucketName => this.deleteOne(bucketName));
        return Promise.all(promises);
    }

    /**
     * Recursively delete all versions of all objects within the bucket
     * @param bucketName
     * @returns {Promise.<T>}
     */
    async empty(bucketName, BypassGovernanceRetention = false) {
        let keyMarker = undefined;
        let versionIdMarker = undefined;
        let isTruncated = true;

        while (isTruncated) {
            const response = await this.s3.send(
                new ListObjectVersionsCommand({
                    Bucket: bucketName,
                    KeyMarker: keyMarker,
                    VersionIdMarker: versionIdMarker,
                }),
            );

            const objects = [...(response.Versions || []), ...(response.DeleteMarkers || [])].map(
                ({ Key, VersionId }) => ({ Key, VersionId }),
            );

            if (objects.length > 0) {
                try {
                    const result = await this.s3.send(
                        new DeleteObjectsCommand({
                            Bucket: bucketName,
                            Delete: {
                                Objects: objects,
                                Quiet: true,
                            },
                            ...(BypassGovernanceRetention && { BypassGovernanceRetention }),
                        }),
                    );
                    if (result.Errors && result.Errors.length > 0) {
                        for (const e of result.Errors) {
                            // eslint-disable-next-line no-console
                            console.warn(
                                `Warning BucketUtility.empty(): failed to delete s3://${bucketName}/${e.Key}` +
                                    ` (versionId=${e.VersionId}): ${e.Code} - ${e.Message}`,
                            );
                        }
                    }
                } catch (err) {
                    // Older cloudserver versions reject DeleteObjects with BadDigest
                    // due to a Content-MD5 integrity check issue. Fall back to individual deletes.
                    if (err.name !== 'BadDigest') {
                        throw err;
                    }
                    await Promise.all(
                        objects.map(({ Key, VersionId }) =>
                            this.s3.send(
                                new DeleteObjectCommand({
                                    Bucket: bucketName,
                                    Key,
                                    VersionId,
                                    ...(BypassGovernanceRetention && { BypassGovernanceRetention }),
                                }),
                            ),
                        ),
                    );
                }
            }

            isTruncated = response.IsTruncated;
            if (isTruncated) {
                keyMarker = response.NextKeyMarker;
                versionIdMarker = response.NextVersionIdMarker;
            }
        }
    }

    emptyMany(bucketNames) {
        const promises = bucketNames.map(bucketName => this.empty(bucketName));
        return Promise.all(promises);
    }

    emptyIfExists(bucketName) {
        return this.bucketExists(bucketName).then(exists => {
            if (exists) {
                return this.empty(bucketName);
            }
            return undefined;
        });
    }

    emptyManyIfExists(bucketNames) {
        const promises = bucketNames.map(bucketName => this.emptyIfExists(bucketName));
        return Promise.all(promises);
    }

    getOwner() {
        return this.s3
            .send(new ListBucketsCommand({}))
            .then(data => data.Owner)
            .catch(err => {
                throw err;
            });
    }
}

module.exports = BucketUtility;
