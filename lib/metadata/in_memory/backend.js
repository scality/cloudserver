import metadata from './metadata';

const metastore = {
    createBucket: (bucketName, bucketMD, cb) => {
        metastore.getBucketAttributes(bucketName, (err, bucket) => {
            // TODO Check whether user already owns the bucket,
            // if so return "BucketAlreadyOwnedByYou"
            // If not owned by user, return "BucketAlreadyExists"
            if (bucket) {
                return cb('BucketAlreadyExists');
            }
            metadata.buckets[bucketName] = bucketMD;
            cb();
        });
    },

    getBucketAttributes: (bucketName, cb) => {
        if (!metadata.buckets[bucketName]) {
            return cb('NoSuchBucket');
        }
        cb(null, metadata.buckets[bucketName]);
    },

    deleteBucket: (bucketName, cb) => {
        if (!metadata.buckets[bucketName]) {
            return cb('NoSuchBucket');
        }
        delete metadata.buckets[bucketName];
        cb(null);
    },

    putObject: (bucketName, objName, objVal, cb) => {
        metastore.getBucketAttributes(bucketName, (err, bucket) => {
            if (err) {
                return cb(err);
            }
            bucket.keyMap[objName] = objVal;
            cb();
        });
    },
};

export default metastore;
