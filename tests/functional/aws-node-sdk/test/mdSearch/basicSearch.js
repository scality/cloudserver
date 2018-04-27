const s3Client = require('./utils/s3SDK');
const { runAndCheckSearch, runIfMongo } = require('./utils/helpers');

const objectKey = 'findMe';
const hiddenKey = 'leaveMeAlone';
const objectTagData = 'item-type=main';
const hiddenTagData = 'item-type=dessert';
const userMetadata = { food: 'pizza' };
const updatedUserMetadata = { food: 'cake' };

runIfMongo('Basic search', () => {
    const bucketName = `basicsearchmebucket${Date.now()}`;
    before(done => {
        s3Client.createBucket({ Bucket: bucketName }, err => {
            if (err) {
                return done(err);
            }
            return s3Client.putObject({ Bucket: bucketName, Key: objectKey,
                Metadata: userMetadata, Tagging: objectTagData }, err => {
                if (err) {
                    return done(err);
                }
                return s3Client.putObject({ Bucket: bucketName,
                    Key: hiddenKey, Tagging: hiddenTagData }, done);
            });
        });
    });

    after(done => {
        s3Client.deleteObjects({ Bucket: bucketName, Delete: { Objects: [
            { Key: objectKey },
            { Key: hiddenKey }],
        } },
            err => {
                if (err) {
                    return done(err);
                }
                return s3Client.deleteBucket({ Bucket: bucketName }, done);
            });
    });

    it('should list object with searched for system metadata', done => {
        const encodedSearch = encodeURIComponent(`key="${objectKey}"`);
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, objectKey, done);
    });

    it('should list object with regex searched for system metadata', done => {
        const encodedSearch = encodeURIComponent('key LIKE "find.*"');
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, objectKey, done);
    });

    it('should list object with regex searched for system metadata with flags',
    done => {
        const encodedSearch = encodeURIComponent('key LIKE "/FIND.*/i"');
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, objectKey, done);
    });

    it('should return empty when no object match regex', done => {
        const encodedSearch = encodeURIComponent('key LIKE "/NOTFOUND.*/i"');
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, null, done);
    });

    it('should list object with searched for user metadata', done => {
        const encodedSearch =
            encodeURIComponent(`x-amz-meta-food="${userMetadata.food}"`);
        return runAndCheckSearch(s3Client, bucketName, encodedSearch,
            objectKey, done);
    });

    it('should list object with searched for tag metadata', done => {
        const encodedSearch =
            encodeURIComponent('tags.item-type="main"');
        return runAndCheckSearch(s3Client, bucketName, encodedSearch,
            objectKey, done);
    });

    it('should return empty listing when no object has user md', done => {
        const encodedSearch =
        encodeURIComponent('x-amz-meta-food="nosuchfood"');
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, null, done);
    });

    describe('search when overwrite object', () => {
        before(done => {
            s3Client.putObject({ Bucket: bucketName, Key: objectKey,
                Metadata: updatedUserMetadata }, done);
        });

        it('should list object with searched for updated user metadata',
            done => {
                const encodedSearch =
                encodeURIComponent('x-amz-meta-food' +
                `="${updatedUserMetadata.food}"`);
                return runAndCheckSearch(s3Client, bucketName, encodedSearch,
                objectKey, done);
            });
    });
});

runIfMongo('Search when no objects in bucket', () => {
    const bucketName = `noobjectbucket${Date.now()}`;
    before(done => {
        s3Client.createBucket({ Bucket: bucketName }, done);
    });

    after(done => {
        s3Client.deleteBucket({ Bucket: bucketName }, done);
    });

    it('should return empty listing when no objects in bucket', done => {
        const encodedSearch = encodeURIComponent(`key="${objectKey}"`);
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, null, done);
    });
});

runIfMongo('Invalid regular expression searches', () => {
    const bucketName = `noobjectbucket${Date.now()}`;
    before(done => {
        s3Client.createBucket({ Bucket: bucketName }, done);
    });

    after(done => {
        s3Client.deleteBucket({ Bucket: bucketName }, done);
    });

    it('should return error if pattern is invalid', done => {
        const encodedSearch = encodeURIComponent('key LIKE "/((helloworld/"');
        const testError = {
            code: 'InternalError',
            message: 'We encountered an internal error. Please try again.',
        };
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, testError, done);
    });

    it('should return error if regex flag is invalid', done => {
        const encodedSearch = encodeURIComponent('key LIKE "/((helloworld/ii"');
        const testError = {
            code: 'InternalError',
            message: 'We encountered an internal error. Please try again.',
        };
        return runAndCheckSearch(s3Client, bucketName,
            encodedSearch, testError, done);
    });
});
