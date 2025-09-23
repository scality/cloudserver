const {
    CreateBucketCommand,
    PutBucketVersioningCommand,
    PutObjectCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');
const s3Client = require('./utils/s3SDK');
const { runAndCheckSearch, removeAllVersions, runIfMongo } =
    require('./utils/helpers');

const userMetadata = { food: 'pizza' };
const updatedMetadata = { food: 'pineapple' };
const masterKey = 'master';

runIfMongo('Search in version enabled bucket', () => {
    const bucketName = `versionedbucket${Date.now()}`;
    const VersioningConfiguration = {
        MFADelete: 'Disabled',
        Status: 'Enabled',
    };
    before(async () => {
        await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
        await s3Client.send(new PutBucketVersioningCommand({ 
            Bucket: bucketName,
            VersioningConfiguration 
        }));
        await s3Client.send(new PutObjectCommand({ 
            Bucket: bucketName,
            Key: masterKey, 
            Metadata: userMetadata 
        }));
    });

    after(async () => {
        try {
            await removeAllVersions(s3Client, bucketName);
            await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
        } catch (err) {
            // eslint-disable-next-line no-console
            console.error('Error in cleanup:', err);
            throw err;
        }
    });

    it('should list just master object with searched for metadata by default', done => {
        const encodedSearch =
        encodeURIComponent(`x-amz-meta-food="${userMetadata.food}"`);
        runAndCheckSearch(s3Client, bucketName, encodedSearch, false, masterKey)
            .then(() => {
                done();
            })
            .catch(done);
    });

    describe('New version overwrite', () => {
        before(async () => {
            await s3Client.send(new PutObjectCommand({ 
                Bucket: bucketName,
                Key: masterKey, 
                Metadata: updatedMetadata 
            }));
        });

        it('should list just master object with updated metadata by default', done => {
            const encodedSearch =
            encodeURIComponent(`x-amz-meta-food="${updatedMetadata.food}"`);
            runAndCheckSearch(s3Client, bucketName, encodedSearch, false, masterKey)
                .then(() => {
                    done();
                })
                .catch(done);
        });

        it('should list all object versions that met search query while specifying versions param', done => {
            const encodedSearch =
                encodeURIComponent('x-amz-meta-food LIKE "pi.*"');
            runAndCheckSearch(s3Client, bucketName,
                encodedSearch, true, [masterKey, masterKey])
                .then(() => {
                    done();
                })
                .catch(done);
        });
    });
});
