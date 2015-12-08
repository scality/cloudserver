import Bucket from './Bucket';
import metadata from './metadata';

const metastore = {
    getBucketAttributes: (bucketName, cb) => {
        if (!metadata.buckets[bucketName]) {
            return cb('NoSuchBucket');
        }
        cb(null, metadata.buckets[bucketName]);
    },
};

export default metastore;
