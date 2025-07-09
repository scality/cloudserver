const { S3Client, HeadBucketCommand, CreateBucketCommand, DeleteBucketCommand, ListObjectVersionsCommand, DeleteObjectCommand, ListBucketsCommand } = require('@aws-sdk/client-s3');
const { fromIni } = require('@aws-sdk/credential-provider-ini');

const projectFixture = require('../fixtures/project');
const getConfig = require('../../test/support/config');

class BucketUtility {
    constructor(profile = 'default', config = {}) {
        const s3Config = getConfig(profile, config);
        this.s3 = new S3Client(s3Config);
    }

    async bucketExists(bucketName) {
        try {
            await this.s3.send(new HeadBucketCommand({ Bucket: bucketName }));
            return true;
        } catch (err) {
            if (err.name === 'NotFound') {
                return false;
            }
            throw err;
        }
    }

    async createOne(bucketName) {
        await this.s3.send(new CreateBucketCommand({ Bucket: bucketName }));
        return bucketName;
    }

    async createOneWithLock(bucketName) {
        await this.s3.send(new CreateBucketCommand({
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        }));
        return bucketName;
    }

    async createMany(bucketNames) {
        return Promise.all(bucketNames.map(bucketName => this.createOne(bucketName)));
    }

    async createRandom(nBuckets = 1) {
        if (nBuckets === 1) {
            const bucketName = projectFixture.generateBucketName();
            return this.createOne(bucketName);
        }

        const bucketNames = projectFixture
            .generateManyBucketNames(nBuckets)
            .sort(() => 0.5 - Math.random()); // Simply shuffle array

        return this.createMany(bucketNames);
    }

    async deleteOne(bucketName) {
        return this.s3.send(new DeleteBucketCommand({ Bucket: bucketName }));
    }

    async deleteMany(bucketNames) {
        return Promise.all(bucketNames.map(bucketName => this.deleteOne(bucketName)));
    }

    /**
     * Recursively delete all versions of all objects within the bucket
     * @param bucketName
     * @returns {Promise.<T>}
     */

    async empty(bucketName) {
        const param = {
            Bucket: bucketName,
        };

        const data = await this.s3.send(new ListObjectVersionsCommand(param));
        const deleteOps = [];
        if (data.Versions) {
            deleteOps.push(...data.Versions.map(object =>
                this.s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: object.Key,
                    VersionId: object.VersionId,
                }))
            ));
        }
        if (data.DeleteMarkers) {
            deleteOps.push(...data.DeleteMarkers.map(object =>
                this.s3.send(new DeleteObjectCommand({
                    Bucket: bucketName,
                    Key: object.Key,
                    VersionId: object.VersionId,
                }))
            ));
        }
        return Promise.all(deleteOps);
    }

    async emptyMany(bucketNames) {
        return Promise.all(bucketNames.map(bucketName => this.empty(bucketName)));
    }

    async emptyIfExists(bucketName) {
        const exists = await this.bucketExists(bucketName);
        if (exists) {
            return this.empty(bucketName);
        }
        return undefined;
    }

    async emptyManyIfExists(bucketNames) {
        return Promise.all(bucketNames.map(bucketName => this.emptyIfExists(bucketName)));
    }

    async getOwner() {
        const data = await this.s3.send(new ListBucketsCommand({}));
        return data.Owner;
    }
}

module.exports = BucketUtility;
