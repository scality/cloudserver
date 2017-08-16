const Promise = require('bluebird');
const { S3 } = require('aws-sdk');
const projectFixture = require('../fixtures/project');
const getConfig = require('../../test/support/config');

class BucketUtility {
    constructor(profile = 'default', config = {}) {
        const s3Config = getConfig(profile, config);

        this.s3 = Promise.promisifyAll(new S3(s3Config));
    }

    createOne(bucketName) {
        return this.s3
            .createBucketAsync({ Bucket: bucketName })
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
            .deleteBucketAsync({ Bucket: bucketName });
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
            .listObjectVersionsAsync(param)
            .then(data =>
                Promise.all(
                    data.Versions
                        .filter(object => !object.Key.endsWith('/'))
                        // remove all objects
                        .map(object =>
                            this.s3.deleteObjectAsync({
                                Bucket: bucketName,
                                Key: object.Key,
                                VersionId: object.VersionId,
                            })
                              .then(() => object)
                        )
                        .concat(data.Versions
                            .filter(object => object.Key.endsWith('/'))
                            // remove all directories
                            .map(object =>
                                this.s3.deleteObjectAsync({
                                    Bucket: bucketName,
                                    Key: object.Key,
                                    VersionId: object.VersionId,
                                })
                                .then(() => object)
                            )
                        )
                        .concat(data.DeleteMarkers
                            .map(object =>
                                 this.s3.deleteObjectAsync({
                                     Bucket: bucketName,
                                     Key: object.Key,
                                     VersionId: object.VersionId,
                                 })
                                 .then(() => object)))
                )
            );
    }

    getOwner() {
        return this.s3
            .listBucketsAsync()
            .then(data => data.Owner);
    }
}

module.exports = BucketUtility;
