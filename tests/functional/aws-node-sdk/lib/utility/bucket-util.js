const {
    S3Client,
    HeadBucketCommand,
    CreateBucketCommand,
    DeleteBucketCommand,
    ListObjectVersionsCommand,
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
        }
       else {
            this.s3 = new S3Client({
                ...s3Config,
                maxAttempts: 0,
             });
       }
    }

    bucketExists(bucketName) {
        return this.s3.send(new HeadBucketCommand({ Bucket: bucketName }))
            .then(() => true)
            .catch(err => {
                if (err.name === 'NotFound') {
                    return false;
                }
                throw err;
            });
    }

    createOne(bucketName) {
        return this.s3.send(new CreateBucketCommand({ Bucket: bucketName }))
            .then(() => bucketName)
            .catch(err => {
                throw err;
            });
    }

    createOneWithLock(bucketName) {
        return this.s3.send(new CreateBucketCommand({
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        })).then(() => bucketName);
    }

    createMany(bucketNames) {
        const promises = bucketNames.map(bucketName =>
            this.createOne(bucketName),
        );
        return Promise.all(promises);
    }

    createRandom(nBuckets = 1) {
        if (nBuckets === 1) {
            const bucketName = projectFixture.generateBucketName();
            return this.createOne(bucketName);
        }
        const bucketNames = projectFixture
            .generateManyBucketNames(nBuckets)
            .sort(() => 0.5 - Math.random());
        return this.createMany(bucketNames);
    }

    deleteOne(bucketName) {
        return this.s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    }

    deleteMany(bucketNames) {
        const promises = bucketNames.map(bucketName =>
            this.deleteOne(bucketName),
        );
        return Promise.all(promises);
    }
    
    /**
     * Recursively delete all versions of all objects within the bucket
     * @param bucketName
     * @returns {Promise.<T>}
     */
    empty(bucketName, BypassGovernanceRetention = false) {
        const param = {
            Bucket: bucketName,
        };

        return this.s3.send(new ListObjectVersionsCommand(param))
            .then(data => Promise.all(
                (data.Versions || [])
                .filter(object => !object.Key.endsWith('/'))
                .map(object =>
                    this.s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: object.Key,
                        VersionId: object.VersionId,
                        ...(BypassGovernanceRetention && { BypassGovernanceRetention }),
                    })).then(() => object)
                )
                .concat((data.Versions || [])
                    .filter(object => object.Key.endsWith('/'))
                    .map(object =>
                        this.s3.send(new DeleteObjectCommand({
                            Bucket: bucketName,
                            Key: object.Key,
                            VersionId: object.VersionId,
                            ...(BypassGovernanceRetention && { BypassGovernanceRetention }),
                        }))
                        .then(() => object)
                    )
                )
                .concat((data.DeleteMarkers || [])
                    .map(object =>
                    this.s3.send(new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: object.Key,
                        VersionId: object.VersionId,
                        ...(BypassGovernanceRetention && { BypassGovernanceRetention }),
                        }))
                        .then(() => object)
                    )
                )
            )
        );
    }

    emptyMany(bucketNames) {
        const promises = bucketNames.map(
            bucketName => this.empty(bucketName)
        );
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
        const promises = bucketNames.map(bucketName =>
            this.emptyIfExists(bucketName),
        );
        return Promise.all(promises);
    }

    getOwner() {
        return this.s3.send(new ListBucketsCommand({}))
            .then(data => data.Owner)
            .catch(err => {
                throw err;
            });
    }
}

module.exports = BucketUtility;
