const bluebird = require('bluebird');
const { S3 } = require('aws-sdk');
const projectFixture = require('../fixtures/project');
const getConfig = require('../../test/support/config');

class BucketUtility {
    constructor(profile = 'default', config = {}) {
        const s3Config = getConfig(profile, config);

        this.s3 = new S3(s3Config);
        this.s3.config.setPromisesDependency(bluebird);
        this.s3.config.update({
            maxRetries: 0,
        });
    }

    createOne(bucketName) {
        return this.s3
            .createBucket({ Bucket: bucketName }).promise()
            .then(() => bucketName);
    }

    createOneWithLock(bucketName) {
        return this.s3.createBucket({
            Bucket: bucketName,
            ObjectLockEnabledForBucket: true,
        }).promise()
        .then(() => bucketName);
    }

    createMany(bucketNames) {
        const promises = bucketNames.map(
            bucketName => this.createOne(bucketName)
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
            .sort(() => 0.5 - Math.random()); // Simply shuffle array

        return this.createMany(bucketNames);
    }

    deleteOne(bucketName) {
        return this.s3
            .deleteBucket({ Bucket: bucketName }).promise();
    }

    deleteMany(bucketNames) {
        const promises = bucketNames.map(
            bucketName => this.deleteOne(bucketName)
        );

        return Promise.all(promises);
    }

    /**
     * Recursively delete all versions of all objects within the bucket
     * @param bucketName
     * @returns {Promise.<T>}
     */

    empty(bucketName) {
        const param = {
            Bucket: bucketName,
        };

        return this.s3
            .listObjectVersions(param).promise()
            .then(data =>
                Promise.all(
                    data.Versions
                        .filter(object => !object.Key.endsWith('/'))
                        // remove all objects
                        .map(object =>
                            this.s3.deleteObject({
                                Bucket: bucketName,
                                Key: object.Key,
                                VersionId: object.VersionId,
                            }).promise()
                              .then(() => object)
                        )
                        .concat(data.Versions
                            .filter(object => object.Key.endsWith('/'))
                            // remove all directories
                            .map(object =>
                                this.s3.deleteObject({
                                    Bucket: bucketName,
                                    Key: object.Key,
                                    VersionId: object.VersionId,
                                }).promise()
                                .then(() => object)
                            )
                        )
                        .concat(data.DeleteMarkers
                            .map(object =>
                                 this.s3.deleteObject({
                                     Bucket: bucketName,
                                     Key: object.Key,
                                     VersionId: object.VersionId,
                                 }).promise()
                                 .then(() => object)))
                )
            );
    }

    emptyMany(bucketNames) {
        const promises = bucketNames.map(
            bucketName => this.empty(bucketName)
        );

        return Promise.all(promises);
    }

    getOwner() {
        return this.s3
            .listBuckets().promise()
            .then(data => data.Owner);
    }
}

module.exports = BucketUtility;
