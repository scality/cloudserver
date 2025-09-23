const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectsCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
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
    before(async () => {
        await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
        await s3Client.send(new PutObjectCommand({ 
            Bucket: bucketName, 
            Key: objectKey,
            Metadata: userMetadata, 
            Tagging: objectTagData 
        }));
        await s3Client.send(new PutObjectCommand({ 
            Bucket: bucketName,
            Key: hiddenKey, 
            Tagging: hiddenTagData 
        }));
    });

    after(async () => {
        await s3Client.send(new DeleteObjectsCommand({ 
            Bucket: bucketName, 
            Delete: { 
                Objects: [
                    { Key: objectKey },
                    { Key: hiddenKey }
                ]
            } 
        }));
        await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should list object with searched for system metadata', done => {
        const encodedSearch = encodeURIComponent(`key="${objectKey}"`);
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
            .then(() => {
                done();
            })
            .catch(done);
    });

    it('should list object with regex searched for system metadata', done => {
        const encodedSearch = encodeURIComponent('key LIKE "find.*"');
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
            .then(() => {
                done();
            })
            .catch(done);
    });

    it('should list object with regex searched for system metadata with flags',
    done => {
        const encodedSearch = encodeURIComponent('key LIKE "/FIND.*/i"');
       runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
           .then(() => {
               done();
           })
           .catch(done);
    });

    it('should return empty when no object match regex', done => {
        const encodedSearch = encodeURIComponent('key LIKE "/NOTFOUND.*/i"');
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, null)
            .then(() => {
                done();
            })
            .catch(done);
    });

    it('should list object with searched for user metadata', done => {
        const encodedSearch =
            encodeURIComponent(`x-amz-meta-food="${userMetadata.food}"`);
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
            .then(() => {
                done();
            })
            .catch(done);
    });

    it('should list object with searched for tag metadata', done => {
        const encodedSearch =
            encodeURIComponent('tags.item-type="main"');
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
            .then(() => {
                done();
            })
            .catch(done);
    });

    it('should return empty listing when no object has user md', done => {
        const encodedSearch =
        encodeURIComponent('x-amz-meta-food="nosuchfood"');
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, null)
            .then(() => {
                done();
            })
            .catch(done);
    });

    describe('search when overwrite object', () => {
        before(async () => {
            await s3Client.send(new PutObjectCommand({ 
                Bucket: bucketName, 
                Key: objectKey,
                Metadata: updatedUserMetadata 
            }));
        });

        it('should list object with searched for updated user metadata',
            done => {
                const encodedSearch =
                encodeURIComponent('x-amz-meta-food' +
                `="${updatedUserMetadata.food}"`);
                runAndCheckSearch(s3Client, bucketName, encodedSearch, false, objectKey)
                    .then(() => {
                        done();
                    })
                    .catch(done);
            });
    });
});

runIfMongo('Search when no objects in bucket', () => {
    const bucketName = `noobjectbucket${Date.now()}`;
    before(async () => {
        await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
    });

    after(async () => {
        await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should return empty listing when no objects in bucket', done => {
        const encodedSearch = encodeURIComponent(`key="${objectKey}"`);
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, null)
            .then(() => {
                done();
            })
            .catch(done);
    });
});

runIfMongo('Invalid regular expression searches', () => {
    const bucketName = `badregex-${Date.now()}`;
    before(async () => {
        await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
    });

    after(async () => {
        await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
    });

    it('should return error if pattern is invalid', done => {
        const encodedSearch = encodeURIComponent('key LIKE "/((helloworld/"');
        const testError = {
            code: 'InvalidArgument',
            message: 'Invalid sql where clause sent as search query',
        };
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, testError)
            .then(() => {
                done();
            })
            .catch(done);
    });
});
